from typing import Annotated
from uuid import UUID

from dishka import FromDishka
from dishka.integrations.fastapi import inject
from fastapi import APIRouter, Header
from starlette.responses import JSONResponse

from app.application.services.idempotency import IdempotencyService
from app.entrypoints.http.schemas.payments import PaymentResponse, PaymentCreate
from app.application.services.payment_service import PaymentServiceProtocol

router = APIRouter(
    tags=["payments"],
)


@router.post("/", status_code=201, response_model=PaymentResponse)
@inject
async def create_payment(
    payload: PaymentCreate,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    payment_service: FromDishka[PaymentServiceProtocol],
    idempotency_service: FromDishka[IdempotencyService],
):
    payload_dict = payload.model_dump(mode="json")
    db_lookup = payment_service.build_idempotency_db_lookup()

    async with idempotency_service(idempotency_key, payload_dict, db_lookup) as guard:
        # TODO Надо ли выносить в __call__ сервиса? Мы же здесь возвращаем JSONResponse
        if guard.has_cached_result and guard.cached_status_code is not None:
            return JSONResponse(
                status_code=guard.cached_status_code,
                content=guard.cached_response,
            )

        created_payment = await payment_service.create(payload, idempotency_key)
        response = PaymentResponse.model_validate(created_payment).model_dump(
            mode="json"
        )

        guard.set_result(status_code=201, response=response)
        return response


@router.get("/{payment_id}", response_model=PaymentResponse)
@inject
async def get_payment(
    payment_id: UUID,
    payment_service: FromDishka[PaymentServiceProtocol],
):
    payment = await payment_service.get(str(payment_id))
    return PaymentResponse.model_validate(payment)
