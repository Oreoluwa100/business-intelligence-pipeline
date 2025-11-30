import pandas as pd
import numpy as np

def generate_products():
     # Product catalog for the e-commerce store
    products = [
        {"product_id": "LAP001", "product_name": "MacBook Pro 14-inch", "category": "Laptops", "price": 1999.99},
        {"product_id": "LAP002", "product_name": "Dell XPS 13", "category": "Laptops", "price": 1299.99},
        {"product_id": "PHN001", "product_name": "iPhone 15 Pro", "category": "Phones", "price": 999.99},
        {"product_id": "PHN002", "product_name": "Samsung Galaxy S24", "category": "Phones", "price": 849.99},
        {"product_id": "ACC001", "product_name": "Wireless Mouse", "category": "Accessories", "price": 49.99},
        {"product_id": "ACC002", "product_name": "Mechanical Keyboard", "category": "Accessories", "price": 129.99},
        {"product_id": "TAB001", "product_name": "iPad Air", "category": "Tablets", "price": 599.99},
        {"product_id": "TAB002", "product_name": "Samsung Galaxy Tab", "category": "Tablets", "price": 449.99}
    ]
    return products

