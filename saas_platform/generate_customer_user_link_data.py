import pandas as pd
import numpy as np
import random

def generate_customer_user_link(customers, users, overlap_percentage = 0.6):
    """
    Creates links between e-commerce customers and SaaS platform users
    Not all customers are platform users, and vice versa
    """
    links = []
    
    # number of links to create
    num_links = min(
        int(len(customers) * overlap_percentage),
        int(len(users) * overlap_percentage)
    )
    
    # Shuffle customer and users data to randomize linking
    shuffled_customers = random.sample(customers, len(customers))
    shuffled_users = random.sample(users, len(users))
    
    for i in range(num_links):
        customer = shuffled_customers[i]
        user = shuffled_users[i]
        
        links.append({
            "link_id": f"L{1000 + i}",
            "customer_id": customer["customer_id"],
            "user_id": user["user_id"],
            "link_type": random.choice(["same_person", "same_company", "affiliated"]),
            "confidence_score": round(random.uniform(0.7, 1.0), 2),  # How sure we are about the link
            "created_date": max(customer["signup_date"], user["signup_date"])  # Link created after both customer and users exist
        })
    
    return links