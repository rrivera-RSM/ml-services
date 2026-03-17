from predictive_attrition.model import (
    load_model,
    get_probabilities,
    classify_from_proba,
)
from predictive_attrition.transformations import build_preprocessor
from predictive_attrition.config import load_config
from predictive_attrition.configs import config_paths as paths
from modules.simulations.infrastructure.repo import SimulationsRepo
from typing import Any, Optional
from datetime import datetime

# load model
model, threshold = load_model(
    path_model=paths.PATH_MODEL,
    experiment_name=paths.experiment_name,
    run_id=paths.run_id,
)

# transformations
cfg = load_config(paths.ruleset_version)
pre = build_preprocessor(
    cfg,
    expenses_out_thrs=paths.expenses_out_thrs,
    b_survey_map_path=paths.b_survey_map,
    b_survey_imputer_path=paths.b_survey_imputer,
    salary_imputer_path=paths.salary_imputer,
    scaler_path=paths.scaler,
    model_expected_cols=model.feature_names_in_,
)


class SimulationService:
    def __init__(self, repo: SimulationsRepo):
        self.repo = repo

    async def predictive_attrition_entrypoint(self, df):
        empl_transform = pre.fit_transform(df)
        empl_prob = get_probabilities(empl_transform, model)
        empl_pred = classify_from_proba(empl_prob, threshold=threshold)
        return empl_pred

    async def get_employee_snapshot_row(
        self,
        employee_id: int,
        new_salary: float,
        new_bonus: Optional[float] = None,
        new_category: Optional[str] = None,
        as_of: Optional[datetime] = None,
    ) -> Optional[dict[str, Any]]:
        """
        Devuelve un diccionario con los datos del empleado necesarios para la simulación.
        """
        return await self.repo.get_employee_snapshot_row(
            employee_id=employee_id,
            as_of=as_of,
            new_salary=new_salary,
            new_bonus=new_bonus,
            new_category=new_category,
        )
