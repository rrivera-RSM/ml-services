from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from infrastructure.db.session import get_db
from modules.simulations.infrastructure.repo import SimulationsRepo
from modules.simulations.application.services import SimulationService


def get_simulations_repo(
    db: AsyncSession = Depends(get_db),
) -> SimulationsRepo:
    return SimulationsRepo(db)


def get_simulations_service(
    repo: SimulationsRepo = Depends(get_simulations_repo),
) -> SimulationService:
    return SimulationService(repo)
