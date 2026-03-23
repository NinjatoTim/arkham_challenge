from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import requests
import os
from typing import Optional

app = FastAPI(title="Frontend Interface")

templates = Jinja2Templates(directory="templates")

API_URL = "http://localhost:8000"

@app.get("/", response_class=HTMLResponse)
async def home(request: Request, facility_id: Optional[int] = None):
    try:
        fac_res = requests.get(f"{API_URL}/facilities")
        facilities = fac_res.json() if fac_res.status_code == 200 else []

        params = {"limit": 50}
        if facility_id: params["facility_id"] = facility_id
        
        data_res = requests.get(f"{API_URL}/data", params=params)
        print(f"DEBUG WEB: Status Code API: {data_res.status_code}")
        print(f"DEBUG WEB: Datos recibidos: {data_res.text[:200]}")
        outages = data_res.json().get("data", [])

        return templates.TemplateResponse(
            request=request,
            name="index.html",
            context={
                "outages": outages,
                "facilities": facilities, 
                "selected_facility": facility_id,
                "error": None
            }
        )
    except Exception as e:
        return templates.TemplateResponse(request=request, name="index.html", 
                                        context={"outages": [], "facilities": [], "error": str(e)})

@app.get("/trigger-refresh")
async def trigger_refresh():
    try:
        response = requests.get(f"{API_URL}/refresh")
        return response.json()
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3000)