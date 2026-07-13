from pydantic import BaseModel, ConfigDict, Field


class StockItem(BaseModel):
    """
    Позиция склада. Инвариант: available и reserved неотрицательны,
    физический остаток = available + reserved.

    Резерв НЕ списывает товар: он перекладывает количество из available
    в reserved. Списание (товар уехал покупателю) происходит на commit:
    reserved уменьшается, available не меняется.
    """

    model_config = ConfigDict(from_attributes=True)

    product_id: str
    # доступно к резервированию
    available: int = Field(ge=0)
    # заблокировано под активные резервы
    reserved: int = Field(default=0, ge=0)
