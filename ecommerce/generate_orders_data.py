import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date, time
import random
import uuid

def generate_orders(num_orders = 5000, customers = None, products = None):
    orders = []
    order_items = []
    
    for _ in range(num_orders):
        order_id = str(uuid.uuid4())  # Generate unique order ID
        random_customer = random.choice(customers)  # Select random customer
        customer_status = random_customer["is_active"]
        if customer_status is False:
            continue  # Skip inactive customers because they can't place orders
        customer_id = random_customer["customer_id"]
        customer_signup_date = random_customer["signup_date"]
        days_since_signup = (datetime.now().date() - customer_signup_date).days 
        if days_since_signup < 1:
            continue  # Skip if customer signed up today (no orders possible)
        # Generate order date with random time, ensuring it's after customer signup
        order_date = datetime.combine(
            (datetime.now() - timedelta(days = random.randint(1, days_since_signup))).date(),
            time(random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
        )
        
        orders.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "order_date": order_date,
            "status": random.choice(["delivered", "shipped", "processing", "cancelled"]),  # Random order status
            "total_amount": 0  # Will be computed from order items
        })
        
        # Generate number of items per order using Poisson distribution (mean = 3 items)
        num_items = max(1, np.random.poisson(lam = 3))         
        order_total = 0
        for _ in range(num_items):
            random_product = random.choice(products)  # Select random product
            quantity = random.randint(1, 3)  # Random quantity between 1-3
            item_total = random_product["price"] * quantity  # Calculate item total
            order_total += item_total  # Add to order total
            
            order_items.append({
                "order_item_id": str(uuid.uuid4()),  # Unique order item ID
                "order_id": order_id,  # Link to parent order
                "product_id": random_product["product_id"],  # Product purchased
                "quantity": quantity,  # Quantity purchased
                "price": random_product["price"],  # Product price at time of order
                "item_total": item_total  # Total for this line item
            })
        
        orders[-1]["total_amount"] = round(order_total, 2)  # Update order total with calculated amount
    
    return orders, order_items  # Return both orders and their line items





