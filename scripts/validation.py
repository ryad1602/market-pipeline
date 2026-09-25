from pydantic import BaseModel, field_validator
from datetime import datetime
from typing import Optional

class StockPriceRecord(BaseModel):
    """
    Définit la forme attendue d'une ligne de prix action.
    Pydantic vérifie automatiquement les types et lève une erreur
    si une valeur ne correspond pas (ex: prix négatif, texte au lieu d'un nombre).
    """
    ticker: str
    price_date: datetime
    open_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None
    close_price: float
    volume: Optional[int] = None

    @field_validator("close_price")
    @classmethod
    def price_must_be_positive(cls, v):
        if v <= 0:
            raise ValueError("close_price doit être strictement positif")
        return v

def validate_records(records: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Sépare les enregistrements valides des invalides.
    Retourne (valides, invalides) — les invalides gardent leur erreur associée.
    """
    valid, invalid = [], []
    for record in records:
        try:
            validated = StockPriceRecord(**record)
            valid.append(record)
        except Exception as e:
            invalid.append({**record, "validation_error": str(e)})
    return valid, invalid
