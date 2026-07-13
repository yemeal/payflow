"""
Read-only Admin API оркестратора (docs/saga-design.md, 9.9).

Мутации (ручной retry, abort, re-drive) сознательно отсутствуют: оператор
не должен уметь двигать сагу мимо машины переходов - это бэклог отдельной
задачи с аудитом. Здесь только наблюдение.

Роутер собирается фабрикой: зависимость авторизации живёт в замыкании, а не
в глобали уровня модуля.
"""

from datetime import timedelta
from typing import Annotated
from uuid import UUID

from dishka import FromDishka
from dishka.integrations.fastapi import inject
from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.application.ports.repositories import (
    SagaRepositoryProtocol,
    SagaTransitionRepositoryProtocol,
)
from app.core.settings import Settings
from app.domain.saga import SagaStatus, utc_now
from app.entrypoints.http.dependencies import create_admin_dependency
from app.entrypoints.http.schemas.admin import (
    SagaDetailResponse,
    SagaResponse,
    SagaTransitionResponse,
)


def create_admin_router(settings: Settings) -> APIRouter:
    require_admin = create_admin_dependency(settings)

    router = APIRouter(
        prefix="/admin/v1",
        tags=["admin"],
        dependencies=[Depends(require_admin)],
    )

    @router.get("/sagas", response_model=list[SagaResponse])
    @inject
    async def list_sagas(
        sagas: FromDishka[SagaRepositoryProtocol],
        saga_type: Annotated[str | None, Query()] = None,
        saga_status: Annotated[SagaStatus | None, Query(alias="status")] = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 50,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> list[SagaResponse]:
        found = await sagas.list_sagas(
            saga_type,
            saga_status.value if saga_status is not None else None,
            limit,
            offset,
        )
        return [SagaResponse.model_validate(saga) for saga in found]

    # объявлен ДО /sagas/{saga_id}: иначе "stuck" уедет в saga_id и упадёт на UUID
    @router.get("/sagas/stuck", response_model=list[SagaResponse])
    @inject
    async def list_stuck_sagas(
        sagas: FromDishka[SagaRepositoryProtocol],
        older_than_seconds: Annotated[int, Query(ge=1)] = 300,
        limit: Annotated[int, Query(ge=1, le=500)] = 50,
    ) -> list[SagaResponse]:
        # порог считаем на сервере: у оператора и БД разные часы, а сравнение
        # идёт с updated_at в naive-UTC
        threshold = utc_now() - timedelta(seconds=older_than_seconds)
        found = await sagas.list_stuck(threshold, limit)
        return [SagaResponse.model_validate(saga) for saga in found]

    @router.get("/sagas/{saga_id}", response_model=SagaDetailResponse)
    @inject
    async def get_saga(
        saga_id: UUID,
        sagas: FromDishka[SagaRepositoryProtocol],
        transitions: FromDishka[SagaTransitionRepositoryProtocol],
    ) -> SagaDetailResponse:
        saga = await sagas.get(saga_id)
        if saga is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="saga not found"
            )

        history = await transitions.list_for_saga(saga_id)
        return SagaDetailResponse(
            saga=SagaResponse.model_validate(saga),
            payload=saga.payload,
            transitions=[
                SagaTransitionResponse.model_validate(transition)
                for transition in history
            ],
        )

    return router
