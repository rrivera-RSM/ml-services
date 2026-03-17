from __future__ import annotations

import pgeocode


from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import select, or_
from sqlalchemy.orm import aliased
from sqlalchemy.ext.asyncio import AsyncSession

from infrastructure.db.models.core.employee import Employee
from infrastructure.db.models.core.employee_history import EmployeeHistory
from infrastructure.db.models.core.office import Office
from infrastructure.db.models.people.evaluation import Evaluation
from infrastructure.db.models.people.positive_impact import PositiveImpact
from infrastructure.db.models.people.salary import Salary
from infrastructure.db.models.core.society import Society
from infrastructure.db.models.core.category import Category
from infrastructure.db.models.people.flexible_compensation import (
    FlexibleCompensation,
)
from infrastructure.db.models.people.expenses import Expenses

from math import radians, sin, cos, sqrt, atan2


def _years_between(start, end):
    if start is None or end is None:
        return None

    # Convertir ambos a date
    if isinstance(start, datetime):
        start = start.date()
    if isinstance(end, datetime):
        end = end.date()

    days = (end - start).days
    return round(days / 365.25, 2)


def _map_gender(gender_id: Optional[int]) -> Optional[str]:
    """
    Temporal.
    Lo correcto es sacar el literal de una tabla catálogo.
    """
    mapping = {
        1: "Masculino",
        2: "Femenino",
    }
    return mapping.get(gender_id)


def haversine_distance_km(lat1, lon1, lat2, lon2):

    R = 6371.0  # Radio de la Tierra en km

    lat1_rad = radians(lat1)
    lon1_rad = radians(lon1)
    lat2_rad = radians(lat2)
    lon2_rad = radians(lon2)

    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = sin(dlat / 2) ** 2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance_km = R * c
    return distance_km


def _calculate_distance_km(
    postal_code1: Optional[str], postal_code2: Optional[str]
) -> Optional[float]:
    if postal_code1 is None or postal_code2 is None:
        return None

    nomi = pgeocode.Nominatim("es")
    loc1 = nomi.query_postal_code(postal_code1)
    loc2 = nomi.query_postal_code(postal_code2)

    if loc1.empty or loc2.empty:
        return None

    lat1, lon1 = loc1.latitude.values[0], loc1.longitude.values[0]
    lat2, lon2 = loc2.latitude.values[0], loc2.longitude.values[0]

    # Aquí podrías usar la fórmula de Haversine para calcular la distancia
    # entre (lat1, lon1) y (lat2, lon2). Por simplicidad, devolveré None.
    return haversine_distance_km(lat1, lon1, lat2, lon2)


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
        Devuelve un diccionario similar al `row` hardcodeado,
        pero construido desde BD con SQLAlchemy.
        """
        as_of = as_of or datetime.now(timezone.utc)

        # -----------------------------
        # Subqueries correlacionadas
        # -----------------------------

        # Historial vigente a fecha as_of
        current_hst_id_sq = (
            select(EmployeeHistory.id)
            .where(
                EmployeeHistory.employee_id == Employee.id,
                EmployeeHistory.start_at <= as_of,
                or_(
                    EmployeeHistory.end_at.is_(None),
                    EmployeeHistory.end_at > as_of,
                ),
            )
            .order_by(
                EmployeeHistory.start_at.desc(), EmployeeHistory.id.desc()
            )
            .limit(1)
            .scalar_subquery()
        )

        # Salario vigente a fecha as_of
        current_salary_id_sq = (
            select(Salary.id)
            .where(
                Salary.employee_id == Employee.id,
                Salary.start_at <= as_of,
                or_(
                    Salary.end_at.is_(None),
                    Salary.end_at > as_of,
                ),
            )
            .order_by(Salary.start_at.desc(), Salary.id.desc())
            .limit(1)
            .scalar_subquery()
        )

        # Última evaluación
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

        # Último impacto positivo
        latest_positive_impact_id_sq = (
            select(PositiveImpact.bol_positive_impact)
            .where(
                PositiveImpact.employee_id == Employee.id,
                PositiveImpact.evaluation_at <= as_of,
            )
            .order_by(
                PositiveImpact.evaluation_at.desc(), PositiveImpact.id.desc()
            )
            .limit(1)
            .scalar_subquery()
        )

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

        #

        # Aliases
        hst = aliased(EmployeeHistory)
        office = aliased(Office)
        sal = aliased(Salary)
        eva = aliased(Evaluation)
        pos = aliased(PositiveImpact)

        stmt = (
            select(
                Employee.id.label("employee_id"),
                Employee.gender_id.label("gender_id"),
                Employee.birth_date.label("birth_date"),
                Employee.joined_at.label("joined_at"),
                Employee.left_at.label("left_at"),
                Employee.postal_code.label("postal_code"),
                Employee.bol_organic_onboard.label("bol_organic_onboard"),
                Employee.postal_code.label("employee_postal_code"),
                Category.name.label("Category"),
                Society.name.label("Society"),
                hst.id.label("history_id"),
                hst.office_id.label("office_id"),
                hst.society_id.label("society_id"),
                hst.department_id.label("department_id"),
                hst.category_id.label("category_id"),
                office.name.label("office_name"),
                office.postal_code.label("office_postal_code"),
                sal.salary.label("current_salary"),
                sal.bonus.label("current_bonus"),
                eva.final_score.label("final_score"),
                pos.bol_positive_impact.label("positive_impact"),
                last_expenses_in_food_sq.label("expenses_food"),
                has_flexible_comp.label("flexible_compensation"),
            )
            .select_from(Employee)
            .outerjoin(hst, hst.id == current_hst_id_sq)
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

        # -----------------------------
        # Derivados en Python
        # -----------------------------
        edad = _years_between(db_row["birth_date"], as_of)
        antiguedad = _years_between(db_row["joined_at"], as_of)
        distance_between_home_and_office_km = _calculate_distance_km(
            db_row["employee_postal_code"], db_row["office_postal_code"]
        )

        current_salary = db_row["current_salary"]
        salary_increment = (new_salary - current_salary) / current_salary

        # OJO: esto depende de la semántica real del campo
        incorporacion_inorganica = None
        if db_row["bol_organic_onboard"] is not None:
            incorporacion_inorganica = int(not db_row["bol_organic_onboard"])

        result = {
            "id": db_row[
                "employee_id"
            ],  # o db_row["external_id"] si ese es tu id de negocio
            "office_name": db_row["office_name"],
            "society_name": db_row["Society"],  # requiere join a Society
            "gender": _map_gender(
                db_row["gender_id"]
            ),  # temporal si no hay tabla catálogo
            "category": (
                new_category if new_category else db_row["Category"]
            ),  # requiere join a catálogo/param
            "seniority": antiguedad,
            "salary": new_salary,
            "salary_increase": salary_increment,
            "bonus": (
                new_bonus if new_bonus is not None else db_row["current_bonus"]
            )
            or 0.0,
            "food": db_row["expenses_food"] or 0.0,
            "is_first_year": int(antiguedad is not None and antiguedad < 1),
            "bol_positive_impact": db_row["positive_impact"] or 0.0,
            "survey_training_technical": 3.91666666666667,  # "La formación en conocimientos técnicos útiles para mi área es buena"
            "survey_training_sofT_skills": 3.25,  # "La formación en habilidades soft (liderazgo, capacidad comercial, comunicación...) es buena"
            "survey_training_digital_skills": 3.75,  # "La formación en competencias y herramientas digitales es buena"
            "survey_open_comunication": 3.33333333333333,  # "En RSM Spain existe una comunicación bidireccional abierta y honesta"
            "survey_leaders_explain_changes": 3.16666666666667,  # "Los líderes de RSM Spain comunican adecuadamente las razones que hay detrás de los cambios relevantes que se realizan en la firma"
            "survey_internal_comm_effective": 3.08333333333333,
        }

        return result
