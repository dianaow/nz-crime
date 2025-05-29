from fastapi import FastAPI, HTTPException, Query, Path, Depends
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
    rank_in_region: Optional[int] = None
    crime_breakdown: Optional[Dict[str, int]] = None
    trend_3m_change: Optional[float] = None
    report_url: str
    widget_embed_code: Optional[str] = None
    summary_text: Optional[str] = None
    tags: Optional[List[str]] = None
    geometry: Optional[Dict[str, Any]] = None
    location_type: Optional[str] = None
    meshblocks: Optional[List[str]] = None
    AU2017_name: Optional[str] = None

class SuburbDetail(SuburbSummary):
    rank_in_region: int
    geometry: Optional[Dict[str, Any]] = None
    crime_breakdown: Dict[str, int]
    trend_3m_change: float
    #crimes: List[Dict[str, Any]]
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
    meshblock_code: Optional[str]

class ReportResponse(BaseModel):
    pdf_url: str
    status: str

class WidgetResponse(BaseModel):
    embed_code: str

class Meshblock(BaseModel):
    id: str
    suburb_id: Optional[str] = None
    geometry: Optional[Dict[str, Any]] = None
    crime_count: Optional[int] = None
    lat: Optional[float] = None
    lng: Optional[float] = None

def get_query_params(
    region: Optional[str] = Query(None, description="Filter by region"),
    min_safety_score: Optional[int] = Query(None, description="Minimum safety score (0-100)"),
    name: Optional[str] = Query(None, description="Filter by suburb name"),
    council: Optional[str] = Query(None, description="Filter by council"),
    crime_trend: Optional[str] = Query(None, description="Filter by crime trend"),
    min_crime_rate: Optional[float] = Query(None, description="Minimum crime rate per 1000"),
    max_crime_rate: Optional[float] = Query(None, description="Maximum crime rate per 1000"),
    min_total_crimes: Optional[int] = Query(None, description="Minimum total crimes in last 12 months"),
    max_total_crimes: Optional[int] = Query(None, description="Maximum total crimes in last 12 months"),
):
    return {k: v for k, v in locals().items() if v is not None}

@app.get("/api/suburbs", response_model=List[SuburbSummary])
async def get_suburbs(
    params: Dict[str, Any] = Depends(get_query_params),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=500, description="Number of records per page")
):
    """Get a list of suburbs with summary crime data"""
    query = supabase.table("suburbs").select("*")
    
    # Apply all filters from query parameters
    for field, value in params.items():
        if field.startswith('min_'):
            actual_field = field[4:]  # Remove 'min_' prefix
            query = query.gte(actual_field, value)
        elif field.startswith('max_'):
            actual_field = field[4:]  # Remove 'max_' prefix
            query = query.lte(actual_field, value)
        else:
            query = query.eq(field, value)
    
    # Apply pagination
    query = query.range((page - 1) * page_size, page * page_size - 1)
    
    response = query.execute()
    return response.data

@app.get("/api/suburbs/{suburb_id}", response_model=SuburbDetail)
async def get_suburb_detail(
    suburb_id: str = Path(..., description="The ID of the suburb"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(100, ge=1, le=1000, description="Number of records per page")
):
    """Get detailed information about a specific suburb"""
    # Get suburb data
    suburb_response = supabase.table("suburbs").select("*").eq("suburb_id", suburb_id).execute()
    
    if not suburb_response.data:
        raise HTTPException(status_code=404, detail="Suburb not found")
    
    suburb = suburb_response.data[0]
    
    # # Get crimes for this suburb with pagination, ordering by date, and limiting to last 12 months
    # crimes_response = (
    #     supabase.table("crimes")
    #     .select("*")
    #     .eq("suburb_id", suburb_id)
    #     .order("victimisation_date", desc=True)  # Order by date descending
    #     .range((page - 1) * page_size, page * page_size - 1)
    #     .execute()
    # )
    
    # # Combine the data
    # suburb["crimes"] = crimes_response.data
    return suburb

def get_crime_query_params(
    offence_category: Optional[str] = Query(None, description="Filter by crime type"),
    district: Optional[str] = Query(None, description="Filter by district"),
    area_unit: Optional[str] = Query(None, description="Filter by area unit"),
    location_type: Optional[str] = Query(None, description="Filter by location type"),
    offence_code: Optional[str] = Query(None, description="Filter by offence code"),
    offence_description: Optional[str] = Query(None, description="Filter by offence description"),
    suburb_id: Optional[str] = Query(None, description="Filter by suburb ID"),
    start_date: Optional[str] = Query(None, description="Filter by start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Filter by end date (YYYY-MM-DD)"),
    month: Optional[str] = Query(None, description="Filter by month (YYYY-MM)")
):
    return {k: v for k, v in locals().items() if v is not None}

@app.get("/api/crimes", response_model=List[CrimeEvent])
async def get_crimes(params: Dict[str, Any] = Depends(get_crime_query_params)):
    """Get crime events with optional filtering"""
    query = supabase.table("crimes").select("*")
    
    # Apply all filters from query parameters
    for field, value in params.items():
        if field == 'month':
            query = query.like("victimisation_date", f"{value}%")
        elif field == 'start_date':
            query = query.gte("victimisation_date", value)
        elif field == 'end_date':
            query = query.lte("victimisation_date", value)
        else:
            query = query.eq(field, value)
    
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

@app.get("/api/meshblocks", response_model=List[Meshblock])
async def get_meshblocks(
    suburb_id: str = Query(..., description="Filter meshblocks by suburb ID")
):
    """Get meshblocks for a specific suburb"""
    # Query meshblocks from Supabase
    response = supabase.table("meshblocks").select("*").eq("suburb_id", suburb_id).execute()
    
    if not response.data:
        raise HTTPException(status_code=404, detail="No meshblocks found for this suburb")
    
    return response.data 