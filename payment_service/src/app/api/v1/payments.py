from typing import Annotated
from uuid import UUID

from dishka import FromDishka
from dishka.integrations.fastapi import inject
from fastapi import APIRouter, Header
from starlette.responses import JSONResponse

from app.models import Payment, PaymentStatus
from app.services.idempotency import IdempotencyService
from app.schemas.payments import PaymentResponse, PaymentCreate
from app.services.payment_service import PaymentServiceProtocol

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
        if guard.has_cached_result:
            return JSONResponse(
                status_code=guard.cached_status_code,
                content=guard.cached_response,
            )

        new_payment = Payment(
            idempotency_key=idempotency_key,
            **payload.model_dump(),
            status=PaymentStatus.PENDING,
        )
        created = await payment_service.create(new_payment)
        response = PaymentResponse.model_validate(created).model_dump(mode="json")

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
