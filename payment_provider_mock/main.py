import asyncio
import logging
import random
from contextlib import asynccontextmanager
from decimal import Decimal

from fastapi import FastAPI, HTTPException, status, BackgroundTasks
from pydantic import BaseModel, Field, ConfigDict
import uuid
import uuid6
from sqlalchemy import String, Float
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mock-provider")

# Database setup
DATABASE_URL = "sqlite+aiosqlite:///./provider.db"
engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


class Base(AsyncAttrs, DeclarativeBase):
    pass


class Transaction(Base):
    __tablename__ = "transactions"

    id: Mapped[str] = mapped_column(String, primary_key=True)  # UUID v7
    amount: Mapped[Decimal] = mapped_column(nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False, default="PENDING")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize DB schema
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database initialized.")
    yield
    await engine.dispose()


app = FastAPI(title="Mock Payment Provider (Async)", lifespan=lifespan)


class PaymentRequest(BaseModel):
    amount: Decimal = Field(gt=0)
    currency: str = Field(min_length=3, max_length=3)


class TransactionResponse(BaseModel):
    id: str
    status: str
    amount: Decimal
    currency: str

    model_config = ConfigDict(from_attributes=True)


async def process_transaction_in_background(tx_id: str):
    logger.info(f"[{tx_id}] Redirecting user to 3DS/payment page...")
    # Simulate user entering code / provider processing
    await asyncio.sleep(5.0)

    roll = random.randint(1, 100)
    # 30% chance to fail
    if roll <= 30:
        final_status = "FAILED"
    else:
        final_status = "COMPLETED"

    logger.info(f"[{tx_id}] Processing finished. Final status: {final_status}")

    async with AsyncSessionLocal() as session:
        tx = await session.get(Transaction, tx_id)
        if tx:
            tx.status = final_status
            await session.commit()


@app.post(
    "/transactions/",
    response_model=TransactionResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_transaction(
    request: PaymentRequest,
    background_tasks: BackgroundTasks,
):
    tx_id = str(uuid6.uuid7())
    logger.info(
        f"[{tx_id}] Received payment request: {request.amount} {request.currency}"
    )

    roll = random.randint(1, 100)

    # 10% долгий таймаут (отвечаем долго или отваливаемся)
    if roll <= 10:
        logger.warning(
            f"[{tx_id}] Simulating timeout error (sleeping for 10 seconds)..."
        )
        await asyncio.sleep(10.0)
        from fastapi import Response

        return Response(content="Gateway Timeout (Simulated)", status_code=504)

    # 50% внутренняя ошибка
    elif roll <= 60:
        logger.error(f"[{tx_id}] Simulating 500 Internal Server Error...")
        from fastapi import Response

        return Response(content="Internal Server Error (Simulated)", status_code=500)

    # Save to DB as PENDING
    async with AsyncSessionLocal() as session:
        new_tx = Transaction(
            id=tx_id, amount=request.amount, currency=request.currency, status="PENDING"
        )
        session.add(new_tx)
        await session.commit()

    # Trigger background task to process it
    background_tasks.add_task(process_transaction_in_background, tx_id)

    return TransactionResponse(
        id=tx_id,
        status="PENDING",
        amount=request.amount,
        currency=request.currency,
    )


@app.get("/transactions/{transaction_id}", response_model=TransactionResponse)
async def get_transaction(transaction_id: str):
    async with AsyncSessionLocal() as session:
        tx = await session.get(Transaction, transaction_id)
        if not tx:
            raise HTTPException(status_code=404, detail="Transaction not found")

        return tx


@app.get("/health")
async def health():
    return {"status": "ok"}
