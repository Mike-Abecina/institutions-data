"""
Pydantic models for all data structures used across the institutions_design_poc project.
"""

from pydantic import BaseModel
from typing import Dict, List, Optional


class NearbyVenue(BaseModel):
    name: str
    venue_type: str  # "restaurant" | "cafe" | "bar"
    distance_walk_min: int
    student_review: Optional[str] = None
    student_reviewer: Optional[str] = None
    rating: Optional[float] = None
    price_level: Optional[str] = None  # "$" | "$$" | "$$$"


class DayInLifeEntry(BaseModel):
    time: str  # "7:00"
    activity: str
    emoji: str
    description: str
    photo_prompt: Optional[str] = None


class CostBreakdown(BaseModel):
    tuition_range_low: int
    tuition_range_high: int
    tuition_comparison: str  # "Cheaper than avg" etc
    rent_range_low: int
    rent_range_high: int
    rent_comparison: str
    weekly_budget: Dict[str, int]  # {"rent": 220, "food": 80, "transport": 25, "fun": 50}
    total_weekly: int


class Scholarship(BaseModel):
    name: str
    amount: str  # "$10K/yr"
    eligibility: Optional[str] = None


class InstitutionCard(BaseModel):
    id: str
    name: str
    city: str
    state: str
    latitude: float
    longitude: float
    institution_type: str  # "University" | "TAFE"
    student_count: Optional[int] = None
    tagline: str
    tagline_reasoning: Optional[str] = None
    hero_gradient: str  # CSS gradient for POC (replace with image later)
    tags: List[str]  # ["Marine Science", "Tropical", "#3 in Australia for Environment"]
    # VIBE tab
    vibe_tags: List[str]  # ["laid-back", "outdoorsy", "tight-knit"]
    campus_mood: Dict[str, int]  # {"study": 42, "social": 67, "chill": 83}
    student_quote: str
    student_quote_author: str
    student_quote_year: Optional[str] = None
    # EATS tab
    walk_score: int
    transit_score: Optional[int] = None
    bike_score: Optional[int] = None
    nearby_venues: List[NearbyVenue]
    cafes_10min: int
    bars_10min: int
    beaches_15min: Optional[int] = None
    # LIFE tab
    day_in_life: List[DayInLifeEntry]
    clubs: List[str]
    transport: Dict[str, str]  # {"bus_to_cbd": "12 min", "airport": "20 min", ...}
    safety_support: List[str]
    # COST tab
    cost: CostBreakdown
    scholarships: List[Scholarship]
    # COMPARE
    top_courses: List[str]
    # QUIZ matching
    vibe_location: str  # "beach" | "city" | "country" | "campus"
    vibe_energy: str  # "creating" | "discovering" | "building" | "helping"
    vibe_weekend: str  # "bushwalk" | "live_music" | "markets" | "netflix"


class ReviewResult(BaseModel):
    domain: str
    passed: bool
    score: float  # 0.0 - 1.0
    feedback: List[str]
    blocking_issues: List[str]
    suggestions: List[str]
