from fastapi import FastAPI, Query, HTTPException
import sqlite3
import pandas as pd
from typing import Optional 
import sys
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, '..'))
sys.path.append(PROJECT_ROOT)
from connector import fetch_nuclear_outages  
from data.storage import run_storage

app = FastAPI(title="Nuclear Outage API")

DATABASE = os.path.join(PROJECT_ROOT, "data", "nuclear_outages.db")

def get_db_connection():
    if not os.path.exists(DATABASE):
        raise HTTPException(status_code=500, detail="Database not found. Run /refresh first.")
    
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row  # allows access by column name
    return conn

# ENDPOINT /data
@app.get("/data")
def get_outages(
    facility_id: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = Query(10, le=100),
    offset: int = 0
):
    conn = get_db_connection()
    
    # dynamic query construction
    query = "SELECT * FROM Outage_report WHERE 1=1"
    params = []

    if facility_id:
        query += " AND facility_id = ?"
        params.append(facility_id)
    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    # pagination
    query += f" LIMIT {limit} OFFSET {offset}"
    
    try:
        cursor = conn.execute(query, params)
        rows = cursor.fetchall()
        # Convert SQLite rows to a dictionary list for JSON response
        data = [dict(row) for row in rows]
        return {"count": len(data), "limit": limit, "offset": offset, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# ENDPOINT /refresh
@app.get("/refresh")
def refresh():
    try:
        fetch_nuclear_outages() 
        print("Updating database")
        message = run_storage()
        return {"status": "success", "message": "Updated data"}
    except Exception as e:
        print(f"Error refresing data")
        raise HTTPException(status_code=500, detail=f"Error al refrescar: {str(e)}")

@app.get("/health")
def health_check():
    health_status = {
        "status": "online",
        "database": "disconnected",
        "storage_file": "missing"
    }
    
    if os.path.exists(DATABASE):
        health_status["storage_file"] = "exists"
        
        try:
            conn = sqlite3.connect(DATABASE)
            conn.execute("SELECT 1") 
            conn.close()
            health_status["database"] = "connected"
        except Exception as e:
            health_status["database"] = f"error: {str(e)}"
            health_status["status"] = "degraded"
    else:
        health_status["status"] = "incomplete"

    return health_status

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)