import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import random
import uuid

def generate_customers(num_customers = 1000):
    customers = []
    for i in range(1, num_customers + 1):
        # Generate random signup date between today and 365 days ago
        signup_date = (datetime.now() - timedelta(days = random.randint(0, 365))).date()
        customers.append({
            "customer_id":f"c{num_customers + i}",  # Unique customer ID
            "email": f"customer{i}@gmail.com",  # Customer email address
            "first_name": f"firstname{i}",  # Customer first name
            "last_name": f"lastname{i}",  # Customer last name
            "signup_date": signup_date,  # Date when customer signed up
            "country": random.choice(["US", "UK", "Canada", "Germany", "Australia"]),  # Customer country
            "is_active": random.choices(["True", "False"], weights = [0.8, 0.2])[0]  # 80% chance customer is active
        })
    return customers


