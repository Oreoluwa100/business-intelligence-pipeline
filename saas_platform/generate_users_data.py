import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import random
import uuid

def generate_users(num_users = 800):
    users = []
    for i in range(1, num_users + 1):
        # Generate random signup date between 1 and 365 days ago
        signup_date = (datetime.now() - timedelta(days = random.randint(1, 365))).date()
        users.append({
            "user_id": f"U{num_users + i}",  # Unique user ID for SaaS platform
            "email": f"users{i}@company.com",  # User email address
            "company_name": f"Company_{random.randint(1000, 9999)}",  # Random company name
            "signup_date": signup_date,  # Date when user signed up for platform
            "initial_plan": random.choice(["free", "basic", "premium", "enterprise"]),  # Starting subscription plan
            "country": random.choice(["US", "UK", "Canada", "Germany", "Australia"]),  # User company country
            "industry": random.choice(["E-commerce", "SaaS", "Retail", "Healthcare", "Finance", "Education"]),  # Company industry
            "employee_count": random.choice(["1-10", "11-50", "51-200", "201-500", "500+"])  # Company size
        })
    
    return users


 