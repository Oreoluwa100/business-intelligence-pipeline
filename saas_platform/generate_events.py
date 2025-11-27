import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date, time
import random
import uuid

def generate_events(users, num_events = 50000):
    events = []
    
    event_types = [
        'login', 'logout', 'dashboard_view', 'report_generated',
        'data_exported', 'alert_created', 'user_invited', 'setting_updated',
        'chart_created', 'filter_applied', 'dashboard_shared', 'tutorial_completed'
    ]
    
    event_categories = {
        'login': 'authentication',
        'logout': 'authentication', 
        'dashboard_view': 'viewing',
        'report_generated': 'creation',
        'data_exported': 'export',
        'alert_created': 'creation',
        'user_invited': 'collaboration',
        'setting_updated': 'configuration',
        'chart_created': 'creation',
        'filter_applied': 'interaction',
        'dashboard_shared': 'collaboration',
        'tutorial_completed': 'onboarding'
    }
    
    for _ in range(num_events):
        user = random.choice(users)
        user_id = user['user_id']
        user_signup_date = user['signup_date']

        available_days = (datetime.now().date() - user_signup_date).days
        
        if available_days < 1:
            continue  
 
        event_date = datetime.now() - timedelta(days = random.randint(1, available_days))
        event_timestamp = datetime.combine(
            event_date.date(),
            time(random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
        )
        event_type = random.choice(event_types)
        event_category = event_categories[event_type]

        event_properties = {}
        if event_type == 'report_generated':
            event_properties = {
                'report_type': random.choice(['sales', 'user_engagement', 'revenue', 'custom']),
                'format': random.choice(['pdf', 'csv', 'excel'])
            }
        elif event_type == 'dashboard_view':
            event_properties = {
                'dashboard_id': f"dashboard_{random.randint(1, 20)}",
                'view_duration_seconds': random.randint(30, 600)
            }
        elif event_type == 'chart_created':
            event_properties = {
                'chart_type': random.choice(['bar', 'line', 'pie', 'table']),
                'data_source': random.choice(['sales', 'users', 'website', 'custom'])
            }
        
        events.append({
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "event_type": event_type,
            "event_category": event_category,
            "event_timestamp": event_timestamp,
            "event_date": event_timestamp.date(),
            "event_properties": event_properties,
            "platform": random.choice(['web', 'mobile', 'api']),
            "session_id": str(uuid.uuid4())
        })
    
    return events

