import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date, time
import random
import uuid

def generate_events(users, num_events = 50000):
    events = []
    
    # Define different types of user events in the SaaS platform
    event_types = [
        "login", "logout", "dashboard_view", "report_generated",
        "data_exported", "alert_created", "user_invited", "setting_updated",
        "chart_created", "filter_applied", "dashboard_shared", "tutorial_completed"
    ]
    
    # Map event types to categories for easier analysis and grouping
    event_categories = {
        "login": "authentication",  
        "logout": "authentication", 
        "dashboard_view": "viewing",  
        "report_generated": "creation",  
        "data_exported": "export", 
        "alert_created": "creation",  
        "user_invited": "collaboration",  
        "setting_updated": "configuration",  
        "chart_created": "creation",  
        "filter_applied": "interaction",  
        "dashboard_shared": "collaboration",  
        "tutorial_completed": "onboarding"  
    }
    
    for _ in range(num_events):
        user = random.choice(users)  # Select random user
        user_id = user["user_id"]
        user_signup_date = user["signup_date"]

        # Calculate how many days user has been active (available for events)
        available_days = (datetime.now().date() - user_signup_date).days
        
        if available_days < 1:
            continue  # Skip users who signed up today (no events possible)
 
        # Generate random event date within user's active period
        event_date = datetime.now() - timedelta(days = random.randint(1, available_days))
        # Add random time to the event date
        event_timestamp = datetime.combine(
            event_date.date(),
            time(random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
        )
        event_type = random.choice(event_types)  # Select random event type
        event_category = event_categories[event_type]  # Get category for the event type

        # Add specific properties based on event type 
        event_properties = {}
        if event_type == "report_generated":
            event_properties = {
                "report_type": random.choice(["sales", "user_engagement", "revenue", "custom"]),
                "format": random.choice(["pdf", "csv", "excel"])
            }
        elif event_type == "dashboard_view":
            event_properties = {
                "dashboard_id": f"dashboard_{random.randint(1, 20)}",  # Random dashboard ID
                "view_duration_seconds": random.randint(30, 600)  # View duration 30sec to 10min
            }
        elif event_type == "chart_created":
            event_properties = {
                "chart_type": random.choice(["bar", "line", "pie", "table"]),  # Chart visualization type
                "data_source": random.choice(["sales", "users", "website", "custom"])  # Data source used
            }
        
        # Create event record with all details
        events.append({
            "event_id": str(uuid.uuid4()),  # Unique event identifier
            "user_id": user_id,  # User who performed the event
            "event_type": event_type,  # Specific action performed
            "event_category": event_category,  # Category for grouping events
            "event_timestamp": event_timestamp,  # Full timestamp with date and time
            "event_date": event_timestamp.date(),  # Date only 
            "event_properties": event_properties,  # Additional event-specific data
            "platform": random.choice(["web", "mobile", "api"]),  # Platform where event occurred
            "session_id": str(uuid.uuid4())  # Session identifier 
        })
    
    return events

