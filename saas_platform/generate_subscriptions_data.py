import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import random
import uuid

def generate_subscriptions(users):
    subscriptions = []
    
    plan_details = {
        "free": {"price": 0, "features": "basic_analytics"},
        "basic": {"price": 29, "features": "standard_analytics"},
        "premium": {"price": 99, "features": "advanced_analytics"},
        "enterprise": {"price": 299, "features": "custom_analytics"}
    }
    
    for user in users:
        user_id = user["user_id"]
        current_plan = user["initial_plan"]
        signup_date = user["signup_date"]
        
        # Initial subscription
        subscriptions.append({
            "subscription_id": str(uuid.uuid4()),
            "user_id": user_id,
            "plan_type": current_plan,
            "start_date": signup_date,
            "end_date": None,
            "price": plan_details[current_plan]["price"],
            "status": "active"
        })
        
        # 30% chance to change plans
        if random.random() >= 0.3:
            continue  # 70% of users keep original plan
        
        # Calculate if user can change plans
        days_since_signup = (datetime.now().date() - signup_date).days
        
        if days_since_signup < 30:
            continue  # User too new to change plans
        
        # User changes plan
        days_after_signup = random.randint(30, days_since_signup)
        change_date = signup_date + timedelta(days = days_after_signup)
        new_plan = random.choice(["free", "basic", "premium", "enterprise"])
        
        # End previous subscription
        subscriptions[-1]["end_date"] = change_date
        subscriptions[-1]["status"] = "ended"
        
        # Start new subscription
        subscriptions.append({
            "subscription_id": str(uuid.uuid4()),
            "user_id": user_id,
            "plan_type": new_plan,
            "start_date": change_date,
            "end_date": None,
            "price": plan_details[new_plan]["price"],
            "status": "active"
        })
    
    return subscriptions

