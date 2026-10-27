import os
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import trino

app = FastAPI(title="Crypto Lakehouse Analytics API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_trino_connection():
    return trino.dbapi.connect(
        host='localhost',
        port=8080,
        user='admin',
        catalog='iceberg',
        schema='market'
    )

@app.get("/", include_in_schema=False)
def serve_frontend():
    # Check current directory, parent directory, and frontend directory
    candidate_paths = [
        "index.html",
        "../index.html",
        "../frontend/index.html",
        "frontend/index.html"
    ]
    for path in candidate_paths:
        if os.path.exists(path):
            return FileResponse(path)
            
    return {"message": "Crypto Lakehouse API is operational. Place index.html in the project root to load the terminal UI."}

@app.get("/api/ohlc/{symbol:path}")
def get_ohlc(
    symbol: str, 
    limit: int = Query(default=50, ge=1, le=500, description="Number of candles to return")
):
    # Parameterize the WHERE predicate, interpolate the validated integer limit
    query = f"""
    WITH ordered_trades AS (
        SELECT 
            date_trunc('minute', event_time) AS bar_time,
            price,
            volume,
            event_time,
            ROW_NUMBER() OVER (PARTITION BY date_trunc('minute', event_time) ORDER BY event_time ASC) AS rn_first,
            ROW_NUMBER() OVER (PARTITION BY date_trunc('minute', event_time) ORDER BY event_time DESC) AS rn_last
        FROM iceberg.market.trades
        WHERE symbol = ?
    )
    SELECT 
        to_unixtime(bar_time) AS timestamp,
        MAX(CASE WHEN rn_first = 1 THEN price END) AS open,
        MAX(price) AS high,
        MIN(price) AS low,
        MAX(CASE WHEN rn_last = 1 THEN price END) AS close,
        COALESCE(ROUND(SUM(volume), 4), 0.0) AS volume
    FROM ordered_trades
    GROUP BY bar_time
    ORDER BY bar_time DESC
    LIMIT {limit}
    """
    
    try:
        with get_trino_connection() as conn:
            cur = conn.cursor()
            cur.execute(query, (symbol,))
            rows = cur.fetchall()
            
            candles = []
            for r in rows:
                if r[0] is not None and r[1] is not None:
                    candles.append({
                        "time": int(r[0]),
                        "open": round(float(r[1]), 2),
                        "high": round(float(r[2]), 2),
                        "low": round(float(r[3]), 2),
                        "close": round(float(r[4]), 2),
                        "volume": round(float(r[5]), 4)
                    })
            
            # Return ordered chronologically (oldest to newest) for chart plotting
            return candles[::-1]
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Trino Query Error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)