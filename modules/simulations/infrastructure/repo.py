from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any, Optional
from sqlalchemy import select, or_, case
from sqlalchemy.orm import aliased
from sqlalchemy.ext.asyncio import AsyncSession

from infrastructure.db.models.core.employee import Employee
from infrastructure.db.models.core.employee_history import EmployeeHistory
from infrastructure.db.models.core.office import Office
from infrastructure.db.models.people.evaluation import Evaluation
from infrastructure.db.models.people.salary import Salary
from infrastructure.db.models.core.society import Society
from infrastructure.db.models.core.category import Category
from infrastructure.db.models.people.flexible_compensation import (
    FlexibleCompensation,
)
from infrastructure.db.models.people.ona_active import OnaActive

from infrastructure.db.models.people.answer import Answer
from infrastructure.db.models.people.expenses import Expenses


# Constants
_GENDER_MAPPING = {1: "Masculino", 2: "Femenino"}
_QUESTION_IDS = [381, 382, 383, 398, 399, 400]
_SURVEY_FIELD_NAMES = [
    "survey_training_technical",
    "survey_training_sofT_skills",
    "survey_training_digital_skills",
    "survey_open_comunication",
    "survey_leaders_explain_changes",
    "survey_internal_comm_effective",
]


def _years_between(start, end):
    if start is None or end is None:
        return None
    if isinstance(start, datetime):
        start = start.date()
    if isinstance(end, datetime):
        end = end.date()
    return round((end - start).days / 365.25, 2)


def _map_gender(gender_id: Optional[int]) -> Optional[str]:
    return _GENDER_MAPPING.get(gender_id)


class SimulationsRepo:

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_employee_snapshot_row(
        self,
        employee_id: int,
        new_salary: float,
        new_bonus: Optional[float] = None,
        new_category: Optional[str] = None,
        as_of: Optional[datetime] = None,
    ) -> Optional[dict[str, Any]]:
        """
        Returns a consolidated employee snapshot as of `as_of`, resolving the effective
        history, salary, evaluation, compensation, expenses, and survey attributes needed
        for simulation and prediction purposes.

        IMPORTANT:
        `as_of` must default to August 31, 2026 (UTC) so that all simulations for the
        attrition prediction algorithm are executed against a single, consistent cutoff date
        immediately prior to the salary review / salary selection process that becomes
        effective on September 1, 2026.

        This default is required to ensure consistency, comparability, reproducibility, and
        business alignment across all simulation runs. It should only be overridden when a
        specific and validated use case requires a different reference date.
        """
        as_of = datetime(
            2026, 8, 31, tzinfo=timezone.utc, hour=21, minute=59, second=59
        )
        as_of_historical = datetime(
            2025, 8, 30, tzinfo=timezone.utc, hour=21, minute=59, second=59
        )

        # Correlated subqueries
        current_hst_id_sq = (
            select(EmployeeHistory.id)
            .where(
                EmployeeHistory.employee_id == Employee.id,
                EmployeeHistory.start_at < as_of,
                or_(
                    EmployeeHistory.end_at.is_(None),
                    EmployeeHistory.end_at >= as_of,
                ),
            )
            .order_by(
                EmployeeHistory.start_at.desc(), EmployeeHistory.id.desc()
            )
            .limit(1)
            .scalar_subquery()
        )

        current_salary_id_sq = (
            select(Salary.id)
            .where(
                Salary.employee_id == Employee.id,
                Salary.start_at <= as_of,
                or_(Salary.end_at.is_(None), Salary.end_at > as_of),
            )
            .order_by(Salary.start_at.desc(), Salary.id.desc())
            .limit(1)
            .scalar_subquery()
        )

        latest_evaluation_id_sq = (
            select(Evaluation.id)
            .where(
                Evaluation.employee_id == Employee.id,
                Evaluation.evaluation_at <= as_of,
            )
            .order_by(Evaluation.evaluation_at.desc(), Evaluation.id.desc())
            .limit(1)
            .scalar_subquery()
        )

        latest_positive_impact_id_sq = (
            select(OnaActive.id)
            .where(
                OnaActive.employee_id == Employee.id,
            )
            .limit(1)
            .scalar_subquery()
        )
        # Positive impact is retrieved from ona_active.degree_centrality

        has_flexible_comp = (
            select(FlexibleCompensation.compensation)
            .where(
                FlexibleCompensation.employee_id == Employee.id,
                FlexibleCompensation.start_at <= as_of,
                or_(
                    FlexibleCompensation.end_at.is_(None),
                    FlexibleCompensation.end_at > as_of,
                ),
            )
            .order_by(
                FlexibleCompensation.start_at.desc(),
                FlexibleCompensation.id.desc(),
            )
            .limit(1)
            .scalar_subquery()
        )

        last_expenses_in_food_sq = (
            select(Expenses.food)
            .where(
                Expenses.employee_id == Employee.id,
                Expenses.payment_at <= as_of,
            )
            .order_by(Expenses.payment_at.desc(), Expenses.id.desc())
            .limit(1)
            .scalar_subquery()
        )

        # Aliases
        hst = aliased(EmployeeHistory)

        office = aliased(Office)
        sal = aliased(Salary)
        eva = aliased(Evaluation)
        pos = aliased(OnaActive)

        def latest_answer_sq(question_id, alias_hst):

            adjusted_category_id = case(
                (alias_hst.category_id == 15726720, 15726718),
                else_=alias_hst.category_id,
            )
            # The change of category id for the following question is due
            # to the fact the survey classifies both managers and directors
            # under the same category, while in the employee history they are
            # differentiated. This adjustment is necessary to correctly retrieve
            # the survey answers for those employees whose historical category is "Director"
            # (15726720) by mapping it to the corresponding category id used in the survey
            # which is "Manager" (15726718).

            # NOTE: remove this adjustment when the survey data is aligned with the employee history,
            # hopefully when we can have an answer inyected directly from the survey system instead
            #    of relying on the historical employee data for the survey answers.

            return (
                select(Answer.value)
                .where(
                    Answer.survey_question_id == question_id,
                    Answer.society_id == alias_hst.society_id,
                    Answer.office_id == alias_hst.office_id,
                    Answer.category_id == adjusted_category_id,
                    Answer.gender_id == Employee.gender_id,
                )
                .order_by(Answer.aud_creation_at.desc(), Answer.id.desc())
                .limit(1)
                .scalar_subquery()
            )

        def previous_year_hst_id_sq(as_of_historical):
            return (
                select(EmployeeHistory.id)
                .where(
                    EmployeeHistory.employee_id == Employee.id,
                    EmployeeHistory.start_at < as_of_historical,
                    or_(
                        EmployeeHistory.end_at.is_(None),
                        EmployeeHistory.end_at >= as_of_historical,
                    ),
                )
                .order_by(
                    EmployeeHistory.start_at.desc(), EmployeeHistory.id.desc()
                )
                .limit(1)
                .scalar_subquery()
            )

        prev_hst = aliased(EmployeeHistory)

        debug_stmt = (
            select(
                Employee.id,
                prev_hst.category_id.label("raw_category_id"),
                case(
                    (prev_hst.category_id == 15726720, 15726718),
                    else_=prev_hst.category_id,
                ).label("adjusted_category_id"),
            )
            .select_from(Employee)
            .outerjoin(
                prev_hst,
                prev_hst.id
                == previous_year_hst_id_sq(as_of_historical=as_of_historical),
            )
            .where(Employee.id == employee_id)
        )
        debug_result = await self.db.execute(debug_stmt)
        debug_row = debug_result.mappings().one_or_none()
        print("DEBUG ROW:", debug_row)

        answer_cols = [
            latest_answer_sq(q, prev_hst).label(f"answer_q{q}")
            for q in _QUESTION_IDS
        ]

        stmt = (
            select(
                Employee.id.label("employee_id"),
                Employee.gender_id.label("gender_id"),
                Employee.birth_date.label("birth_date"),
                Employee.joined_at.label("joined_at"),
                Employee.left_at.label("left_at"),
                Employee.postal_code.label("employee_postal_code"),
                Employee.bol_organic_onboard.label("bol_organic_onboard"),
                Category.name.label("category"),
                Society.name.label("society_name"),
                hst.id.label("history_id"),
                hst.office_id.label("office_id"),
                hst.society_id.label("society_id"),
                hst.department_id.label("department_id"),
                hst.category_id.label("category_id"),
                prev_hst.category_id.label("prev_category_id"),
                prev_hst.society_id.label("prev_society_id"),
                prev_hst.department_id.label("prev_department_id"),
                prev_hst.office_id.label("prev_office_id"),
                office.name.label("office_name"),
                office.postal_code.label("office_postal_code"),
                sal.salary.label("current_salary"),
                sal.bonus.label("current_bonus"),
                eva.final_score.label("final_score"),
                pos.degree_centrality.label("positive_impact"),
                last_expenses_in_food_sq.label("expenses_food"),
                has_flexible_comp.label("flexible_compensation"),
                *answer_cols,
            )
            .select_from(Employee)
            .outerjoin(hst, hst.id == current_hst_id_sq)
            .outerjoin(
                prev_hst,
                prev_hst.id
                == previous_year_hst_id_sq(as_of_historical=as_of_historical),
            )
            .outerjoin(office, office.id == hst.office_id)
            .outerjoin(Category, Category.id == hst.category_id)
            .outerjoin(sal, sal.id == current_salary_id_sq)
            .outerjoin(eva, eva.id == latest_evaluation_id_sq)
            .outerjoin(pos, pos.id == latest_positive_impact_id_sq)
            .outerjoin(Society, Society.id == hst.society_id)
            .where(Employee.id == employee_id)
        )

        result = await self.db.execute(stmt)
        db_row = result.mappings().one_or_none()

        if db_row is None:
            return None

        antiguedad = _years_between(db_row["joined_at"], as_of)
        current_salary = db_row["current_salary"]
        salary_increment = (
            (new_salary - current_salary) / current_salary
            if current_salary
            else 0
        )
        answers_sq = [db_row.get(f"answer_q{q}") for q in _QUESTION_IDS]

        # Build survey answers dict
        survey_answers = {
            name: (answers_sq[i] if answers_sq else 0.0)
            for i, name in enumerate(_SURVEY_FIELD_NAMES)
        }

        return {
            "id": db_row["employee_id"],
            "office_name": db_row["office_name"],
            "society_name": db_row["society_name"],
            "gender": _map_gender(db_row["gender_id"]),
            "category": new_category or db_row["category"],
            "seniority": antiguedad,
            "salary": new_salary,
            "salary_increase": salary_increment,
            "bonus": (
                new_bonus
                if new_bonus is not None and new_bonus > 4000
                else db_row["current_bonus"]
            )
            or 0.0,
            "food": db_row["expenses_food"] or 0.0,
            "is_first_year": int(antiguedad is not None and antiguedad <= 1),
            "bol_positive_impact": (
                1
                if db_row["positive_impact"] is not None
                and db_row["positive_impact"] >= 10
                else 0
            ),
            **survey_answers,
        }


