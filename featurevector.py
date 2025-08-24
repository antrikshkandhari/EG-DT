import sqlite3
import json

DB_NAME = 'tradeapp.db'

industry_encoder: dict[str, int] = {}
sector_encoder: dict[str, int] = {}

def _encode_categorical(value: str, mapping: dict[str, int]) -> int:
    if value not in mapping:
        mapping[value] = len(mapping) + 1
    return mapping[value]

def _encode_bucket(value: float, buckets: list[tuple[float, float, int]]) -> int:
    for low, high, code in buckets:
        if low <= value < high:
            return code
    return 0

SHORT_INTEREST_BUCKETS = [
    (0, 5, 1),
    (5, 10, 2),
    (10, 15, 3),
    (15, 20, 4),
    (20, float('inf'), 5),
]

MARKET_CAP_BUCKETS = [
    (0, 1_000_000_000, 1),
    (1_000_000_000, 10_000_000_000, 2),
    (10_000_000_000, 50_000_000_000, 3),
    (50_000_000_000, 200_000_000_000, 4),
    (200_000_000_000, float('inf'), 5),
]

SHARE_FLOAT_BUCKETS = [
    (0, 10_000_000, 1),
    (10_000_000, 50_000_000, 2),
    (50_000_000, 100_000_000, 3),
    (100_000_000, 500_000_000, 4),
    (500_000_000, float('inf'), 5),
]

BUCKETS = {
    'short_interest': SHORT_INTEREST_BUCKETS,
    'market_cap': MARKET_CAP_BUCKETS,
    'share_float': SHARE_FLOAT_BUCKETS,
}

def encode_fundamentals(data: dict, buckets: dict) -> list[int]:
    return [
        _encode_bucket(data.get('short_interest', 0), buckets.get('short_interest', [])),
        _encode_categorical(data.get('Industry', ''), industry_encoder),
        _encode_categorical(data.get('Sector', ''), sector_encoder),
        _encode_bucket(data.get('market_cap', 0), buckets.get('market_cap', [])),
        _encode_bucket(data.get('share_float', 0), buckets.get('share_float', [])),
    ]

def encode_all(db_path: str = DB_NAME) -> None:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT Ticker, short_interest, Industry, Sector, market_cap, share_float
            FROM stock_data
            """
        )
        rows = cursor.fetchall()
        for row in rows:
            ticker, short_int, industry, sector, market_cap, share_float = row
            data = {
                'short_interest': short_int or 0,
                'Industry': industry or '',
                'Sector': sector or '',
                'market_cap': market_cap or 0,
                'share_float': share_float or 0,
            }
            vector = encode_fundamentals(data, BUCKETS)
            cursor.execute(
                "UPDATE stock_data SET feature_vector = ? WHERE Ticker = ?",
                (json.dumps(vector), ticker),
            )
        conn.commit()

if __name__ == '__main__':
    encode_all()
