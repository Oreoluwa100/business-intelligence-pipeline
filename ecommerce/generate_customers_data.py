import pandas as pd
import numpy as mp
from datetime import datetime, timedelta, date
import random
import uuid

def generate_customers(num_customers = 1000):
    customers = []
    for i in range(1, num_customers + 1):
        signup_date = (datetime.now() - timedelta(days = random.randint(0, 365))).date()
        customers.append({
            "customer_id":f"c{num_customers + i}",
            "email": f"customer{i}@gmail.com",
            "first_name": f"firstname{i}",
            "last_name": f"lastname{i}",
            "signup_date": signup_date,
            "country": random.choice(["US", "UK", "Canada", "Germany", "Australia"]),
            "is_active": random.choices(["True", "False"], weights = [0.8, 0.2])[0]
        })
    return customers

print(pd.DataFrame(generate_customers(1000)).iloc[0:5,:])
