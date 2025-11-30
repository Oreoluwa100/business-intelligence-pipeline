import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import random
import uuid

def generate_subscriptions(users):
    subscriptions = []
    
    # Define subscription plan details with pricing and features
    plan_details = {
        "free": {"price": 0, "features": "basic_analytics"},  
        "basic": {"price": 29, "features": "standard_analytics"},  
        "premium": {"price": 99, "features": "advanced_analytics"},  
        "enterprise": {"price": 299, "features": "custom_analytics"}  
    }
    
    for user in users:
        user_id = user["user_id"]
        current_plan = user["initial_plan"]  # user's starting plan
        signup_date = user["signup_date"]
        
        # Create initial subscription at signup
        subscriptions.append({
            "subscription_id": str(uuid.uuid4()),  # Unique subscription ID
            "user_id": user_id,  # Link to user
            "plan_type": current_plan,  # Starting plan type
            "start_date": signup_date,  # Subscription start date (same as signup)
            "end_date": None,  # No end date for active subscription
            "price": plan_details[current_plan]["price"],  # Plan price from plan_details
            "status": "active"  # Initial status is active
        })
        
        # 30% chance to change plans (70% keep original plan)
        if random.random() >= 0.3:
            continue  # Skip plan change for 70% of users
        
        # Calculate if user has been around long enough to change plans
        days_since_signup = (datetime.now().date() - signup_date).days
        
        if days_since_signup < 30:
            continue  # Skip users who signed up less than 30 days ago
        
        days_after_signup = random.randint(30, days_since_signup)
        change_date = signup_date + timedelta(days = days_after_signup)  # Calculate change date
        new_plan = random.choice(["free", "basic", "premium", "enterprise"])  # Random new plan
        
        # End previous subscription on the change date
        subscriptions[-1]["end_date"] = change_date
        subscriptions[-1]["status"] = "ended"  # Mark previous subscription as ended
        
        # Start new subscription on the same change date
        subscriptions.append({
            "subscription_id": str(uuid.uuid4()),  # New unique subscription ID
            "user_id": user_id,  # Same user
            "plan_type": new_plan,  # New plan type
            "start_date": change_date,  # Start date of new subscription
            "end_date": None,  # No end date for current active subscription
            "price": plan_details[new_plan]["price"],  # Price of new plan
            "status": "active"  # New subscription is active
        })
    
    return subscriptions

