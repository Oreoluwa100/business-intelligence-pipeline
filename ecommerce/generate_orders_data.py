import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date, time
import random
import uuid

def generate_orders(num_orders = 5000, customers = None, products = None):
    orders = []
    order_items = []
    
    for _ in range(num_orders):
        order_id = str(uuid.uuid4())
        random_customer = random.choice(customers)
        customer_status = random_customer["is_active"]
        if customer_status is False:
            continue
        customer_id = random_customer["customer_id"]
        customer_signup_date = random_customer["signup_date"]
        days_since_signup = (datetime.now().date() - customer_signup_date).days 
        if days_since_signup < 1:
            continue
        order_date = datetime.combine(
            (datetime.now() - timedelta(days = random.randint(1, days_since_signup))).date(),
            time(random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
        )
        
        orders.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "order_date": order_date,
            "status": random.choice(["delivered", "shipped", "processing", "cancelled"]),
            "total_amount": 0
        })
        
        num_items = max(1, np.random.poisson(lam = 3))         
        order_total = 0
        for _ in range(num_items):
            random_product = random.choice(products)
            quantity = random.randint(1, 3)
            item_total = random_product["price"] * quantity
            order_total += item_total
            
            order_items.append({
                "order_item_id": str(uuid.uuid4()),
                "order_id": order_id,
                "product_id": random_product["product_id"],
                "quantity": quantity,
                "price": random_product["price"],
                "item_total": item_total
            })
        
        orders[-1]["total_amount"] = round(order_total, 2)
    
    return orders, order_items





