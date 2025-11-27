import pandas as pd
import numpy as mp
from datetime import datetime, timedelta, date
import random
import uuid

def generate_users(num_users = 800):
    users = []
    for i in range(1, num_users + 1):
        signup_date = (datetime.now() - timedelta(days = random.randint(1, 365))).date()
        users.append({
            "user_id": f"U{num_users + i}",
            "email": f"users{i}@company.com",
            "company_name": f"Company_{random.randint(1000, 9999)}",
            "signup_date": signup_date,
            "initial_plan": random.choice(['free', 'basic', 'premium', 'enterprise']),
            "country": random.choice(['US', 'UK', 'Canada', 'Germany', 'Australia']),
            "industry": random.choice(['E-commerce', 'SaaS', 'Retail', 'Healthcare', 'Finance', 'Education']),
            "employee_count": random.choice(['1-10', '11-50', '51-200', '201-500', '500+'])
        })
    
    return users

#users = generate_users(800)
#print(pd.DataFrame(users))
 