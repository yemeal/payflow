import asyncio
import random
import logging
from fastapi import FastAPI, Response, status
from pydantic import BaseModel, Field
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mock-provider")

app = FastAPI(title="Mock Payment Provider")

class PaymentRequest(BaseModel):
    amount: float = Field(gt=0)
    currency: str = Field(min_length=3, max_length=3)
    card_number: str = Field(default="1234567812345678")
    customer_id: str

@app.post("/process-payment")
async def process_payment(request: PaymentRequest):
    logger.info(f"Received payment request for amount: {request.amount} {request.currency}, customer: {request.customer_id}")
    
    roll = random.randint(1, 100)
    
    # Case 1: Timeout (15% chance)
    if roll <= 15:
        logger.warning("Simulating timeout error (sleeping for 6 seconds)...")
        await asyncio.sleep(6.0)
        return {"status": "success", "transaction_id": str(uuid.uuid4()), "note": "delayed"}
        
    # Case 2: Internal Server Error (55% chance, total 70% failure rate)
    elif roll <= 70:
        logger.error("Simulating 500 Internal Server Error...")
        return Response(
            content="Internal Server Error (Simulated)",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            media_type="text/plain"
        )
        
    # Case 3: Success (30% chance)
    else:

        tx_id = f"mock_tx_{uuid.uuid4().hex[:12]}"
        logger.info(f"Payment successful. Generated Transaction ID: {tx_id}")
        return {
            "status": "success",
            "transaction_id": tx_id
        }

@app.get("/health")
async def health():
    return {"status": "ok"}
