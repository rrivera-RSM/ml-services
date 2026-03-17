import pandas as pd

from fastapi import APIRouter, Query, Depends
from modules.simulations.application.services import SimulationService
from api.deps import get_simulations_service
from typing import Optional

predictive_attrition_router = APIRouter(tags=["Predictive Attrition"])


@predictive_attrition_router.post("/simulate")
async def main_simulations(
    employee_id: int = Query(..., description="ID del empleado a simular"),
    new_salary: float = Query(
        ..., description="Nuevo salario para la simulación"
    ),
    new_bonus: Optional[float] = Query(
        None, description="Nuevo bonus para la simulación (opcional)"
    ),
    new_category: Optional[str] = Query(
        None,
        description="Nueva categoría profesional en caso de promocion",
    ),
    service: SimulationService = Depends(get_simulations_service),
):
    # datos de entrada
    row = await service.get_employee_snapshot_row(
        employee_id=employee_id,
        new_salary=new_salary,
        new_bonus=new_bonus,
        new_category=new_category,
    )
    df = pd.DataFrame([row])
    empl_result = await service.predictive_attrition_entrypoint(df=df)
    return empl_result
