from fastapi import FastAPI, HTTPException, Query, Path
from typing import List, Optional, Dict, Any
from datetime import date
from pydantic import BaseModel
from supabase import create_client, Client
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize FastAPI app
app = FastAPI(title="NZ Crime API", description="API for accessing New Zealand crime data")

# Initialize Supabase client
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")
if not supabase_url or not supabase_key:
    raise ValueError("Supabase credentials not found in environment variables")
supabase: Client = create_client(supabase_url, supabase_key)

# Pydantic models for request/response validation
class SuburbSummary(BaseModel):
    suburb_id: str
    name: str
    region: str
    council: str
    lat: float
    lng: float
    safety_score: float
    crime_rate_per_1000: float
    crime_trend: str
    total_crimes_12m: int
    most_common_crime: str
    report_url: str

class SuburbDetail(SuburbSummary):
    rank_in_region: int
    geometry: Optional[Dict[str, Any]] = None
    crime_breakdown: Dict[str, int]
    trend_3m_change: float
    crimes: List[Dict[str, Any]]
    #downloadable_report_url: str
    widget_embed_code: str
    tags: List[str]

class CrimeEvent(BaseModel):
    event_id: str
    suburb_id: str
    offence_code: str
    offence_description: str
    offence_category: str
    victimisation_date: date
    district: str
    area_unit: str
    meshblock_code: Optional[str]
    location_type: str

class ReportResponse(BaseModel):
    pdf_url: str
    status: str

class WidgetResponse(BaseModel):
    embed_code: str

@app.get("/api/suburbs", response_model=List[SuburbSummary])
async def get_suburbs(
    region: Optional[str] = Query(None, description="Filter by region"),
    min_safety_score: Optional[int] = Query(None, description="Minimum safety score (0-100)")
):
    """Get a list of suburbs with summary crime data"""
    query = supabase.table("suburbs").select("*")
    
    if region:
        query = query.eq("region", region)
    if min_safety_score is not None:
        query = query.gte("safety_score", min_safety_score)
    
    response = query.execute()
    print(f"Query response: {response}")  # Debug print
    return response.data

@app.get("/api/suburbs/{suburb_id}", response_model=SuburbDetail)
async def get_suburb_detail(
    suburb_id: str = Path(..., description="The ID of the suburb")
):
    """Get detailed information about a specific suburb"""
    # Get suburb data
    suburb_response = supabase.table("suburbs").select("*").eq("suburb_id", suburb_id).execute()
    print(f"Suburb response: {suburb_response}")  # Debug print
    
    if not suburb_response.data:
        raise HTTPException(status_code=404, detail="Suburb not found")
    
    suburb = suburb_response.data[0]
    
    # Get crimes for this suburb
    crimes_response = supabase.table("crimes").select("*").eq("suburb_id", suburb_id).execute()
    print(f"Crimes response: {crimes_response}")  # Debug print
    
    # Combine the data
    suburb["crimes"] = crimes_response.data
    return suburb

@app.get("/api/crimes", response_model=List[CrimeEvent])
async def get_crimes(
    type: Optional[str] = Query(None, description="Filter by crime type"),
    region: Optional[str] = Query(None, description="Filter by region"),
    month: Optional[str] = Query(None, description="Filter by month (YYYY-MM)")
):
    """Get crime events with optional filtering"""
    query = supabase.table("crimes").select("*")
    
    if type:
        query = query.eq("offence_category", type)
    if region:
        query = query.eq("district", region)
    if month:
        query = query.like("victimisation_date", f"{month}%")
    
    response = query.execute()
    return response.data

@app.get("/api/reports/{suburb_id}", response_model=ReportResponse)
async def get_suburb_report(
    suburb_id: str = Path(..., description="The ID of the suburb")
):
    """Get a suburb's crime report (PDF)"""
    # Check if suburb exists
    suburb_response = supabase.table("suburbs").select("report_url").eq("suburb_id", suburb_id).execute()
    if not suburb_response.data:
        raise HTTPException(status_code=404, detail="Suburb not found")
    
    # In a real implementation, you would check if the user has purchased the report
    # and generate/retrieve the PDF accordingly
    return {
        "pdf_url": suburb_response.data[0]["report_url"],
        "status": "not_purchased"  # This would be dynamic based on user's purchase status
    }

@app.get("/api/widgets/{suburb_id}", response_model=WidgetResponse)
async def get_suburb_widget(
    suburb_id: str = Path(..., description="The ID of the suburb")
):
    """Get an embeddable widget for a suburb's safety score"""
    # Check if suburb exists
    suburb_response = supabase.table("suburbs").select("widget_embed_code").eq("suburb_id", suburb_id).execute()
    if not suburb_response.data:
        raise HTTPException(status_code=404, detail="Suburb not found")
    
    return {
        "embed_code": suburb_response.data[0]["widget_embed_code"]
    } 