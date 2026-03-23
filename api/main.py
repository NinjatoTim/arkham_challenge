from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import FastAPI, Query, HTTPException
import sqlite3
import requests
import os
import sys
from typing import Optional

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, '..'))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

try:
    import connector  
    from data import storage  
    print("DEBUG: Internal connections successfully established.")
except ImportError as e:
    print(f"DEBUG: Error importing modules: {e}")
    from connector import fetch_nuclear_outages
    from data.storage import run_storage

DATABASE = os.path.join(PROJECT_ROOT, "data", "nuclear_outages.db")

app = FastAPI(title="Frontend Interface")
templates = Jinja2Templates(directory="templates")
API_URL = "http://localhost:8000"

def get_db_connection():
    if not os.path.exists(DATABASE):
        raise HTTPException(status_code=500, detail="Database not found.")
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn

# /DATA 

@app.get("/data")
def get_outages(
    facility_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = Query(50, le=100)
):
    conn = get_db_connection()
    query = "SELECT * FROM Outage_report WHERE 1=1"
    params = []

    if facility_id and facility_id.strip() not in ["", "None"]:
        query += " AND CAST(facility_id AS TEXT) = ?"
        params.append(facility_id.strip())

    if start_date and start_date.strip():
        query += " AND date >= ?"
        params.append(start_date)

    if end_date and end_date.strip():
        query += " AND date <= ?"
        params.append(end_date)

    query += f" LIMIT {limit}"
    
    try:
        cursor = conn.execute(query, params)
        data = [dict(row) for row in cursor.fetchall()]
        return {"data": data}
    finally:
        conn.close()

#  /FACILITY
@app.get("/facilities")
def get_facilities():
    conn = get_db_connection()
    try:
        cursor = conn.execute("SELECT DISTINCT facility_id, facility_name FROM Facility ORDER BY facility_name")
        return [{"id": r["facility_id"], "name": r["facility_name"]} for r in cursor.fetchall()]
    except:
        return []
    finally:
        conn.close()
# Some endpoint to check connection with the API
@app.get("/health")
def health_check():
    db_exists = os.path.exists(DATABASE)
    status = {"status": "online", "database": "disconnected", "storage_file": "exists" if db_exists else "missing"}
    if db_exists:
        try:
            conn = sqlite3.connect(DATABASE); conn.execute("SELECT 1"); conn.close()
            status["database"] = "connected"
        except Exception as e: status["status"] = "degraded"; status["database"] = str(e)
    return status

# /REFRESH
@app.get("/refresh")
def refresh():
    try:
        fetch_nuclear_outages() 
        run_storage()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)