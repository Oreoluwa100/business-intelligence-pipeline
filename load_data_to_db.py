def load_to_neon():
    """
    This ETL Pipeline generates simulated data and load into Neon PostgreSQL
    Organizes data into separate schemas for better data management
    """

    # Import all data generation functions
    from ecommerce.generate_customers_data import generate_customers
    from ecommerce.generate_products_data import generate_products
    from ecommerce.generate_orders_data import generate_orders
    from saas_platform.generate_users_data import generate_users
    from saas_platform.generate_subscriptions_data import generate_subscriptions
    from saas_platform.generate_events_data import generate_events
    from saas_platform.generate_customer_user_link_data import generate_customer_user_link

    import pandas as pd
    import psycopg2
    from dotenv import load_dotenv
    load_dotenv()
    import os
    import json   
    
    host = os.getenv("NEON_HOST")
    user = os.getenv("NEON_USER")
    password = os.getenv("NEON_PASSWORD")
    database = os.getenv("NEON_DATABASE")
    port = os.getenv("NEON_PORT")

     # Connect to Neon PostgreSQL database
    def connect_to_database():
        cnx = psycopg2.connect(
            host = host,
            dbname = database,
            user = user,
            password = password,
            port = port,
            sslmode = "require",
            connect_timeout = 10,    # Fail fast if can't connect
            keepalives = 1,          # Keep connection alive
            keepalives_idle = 30,    # Send keepalive after 30s idle
            keepalives_interval = 5, # Retry every 5s if no response
            keepalives_count = 3     # Max 3 retries before closing
        )
        cursor = cnx.cursor()
        return cnx, cursor
    cnx, cursor = connect_to_database()
    if cnx is None or cursor is None:
        print("Failed to connect to Neon database")
    else:
        print("Connected to Neon PostgreSQL successfully!")
    
    # Generate all data
    print("Generating simulated business data...")
    
    print("Generating 1,000 ecommerce customers...")
    customers = generate_customers(1000)
    
    print("Generating 8 products...")
    products = generate_products()
    
    print("Generating 5,000 orders...")
    orders, order_items = generate_orders(5000, customers, products)
    
    print("Generating 800 SaaS platform users...")
    users = generate_users(800)
    
    print("Generating user subscriptions...")
    subscriptions = generate_subscriptions(users)
    
    print("Generating 50,000 platform events...")
    events = generate_events(users, 50000)
    
    print("Generating customer-user links...")
    links = generate_customer_user_link(customers, users)

     # Create schemas and tables
    cursor.execute("""CREATE SCHEMA IF NOT EXISTS ecommerce""")  # Ecommerce specific tables
    cursor.execute("""CREATE SCHEMA IF NOT EXISTS saas_platform""")  # SaaS platform tables
    cursor.execute("""CREATE SCHEMA IF NOT EXISTS analytics""") # Analytics tables

    cnx.commit()
    print("Created schemas: ecommerce, saas_platform, analytics")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ecommerce.customers (
            customer_id VARCHAR(50) PRIMARY KEY,
            email VARCHAR(100),
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            signup_date DATE,
            country VARCHAR(50),
            is_active VARCHAR(10)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ecommerce.products (
            product_id VARCHAR(50) PRIMARY KEY,
            product_name VARCHAR(100),
            category VARCHAR(50),
            price DECIMAL(10,2)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ecommerce.orders (
            order_id UUID PRIMARY KEY,
            customer_id VARCHAR(50),
            order_date TIMESTAMP,
            status VARCHAR(50),
            total_amount DECIMAL(10,2)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ecommerce.order_items (
            order_item_id UUID PRIMARY KEY,
            order_id UUID,
            product_id VARCHAR(50),
            quantity INTEGER,
            price DECIMAL(10,2),
            item_total DECIMAL(10,2)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS saas_platform.users (
            user_id VARCHAR(50) PRIMARY KEY,
            email VARCHAR(100),
            company_name VARCHAR(100),
            signup_date DATE,
            initial_plan VARCHAR(50),
            country VARCHAR(50),
            industry VARCHAR(50),
            employee_count VARCHAR(50)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS saas_platform.subscriptions (
            subscription_id UUID PRIMARY KEY,
            user_id VARCHAR(50),
            plan_type VARCHAR(50),
            start_date DATE,
            end_date DATE,
            price DECIMAL(10,2),
            status VARCHAR(50)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS saas_platform.events (
            event_id UUID PRIMARY KEY,
            user_id VARCHAR(50),
            event_type VARCHAR(50),
            event_category VARCHAR(50),
            event_timestamp TIMESTAMP,
            event_date DATE,
            event_properties JSONB,
            platform VARCHAR(50),
            session_id UUID
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS saas_platform.customer_user_link (
            link_id VARCHAR(50) PRIMARY KEY,
            customer_id VARCHAR(50),
            user_id VARCHAR(50),
            link_type VARCHAR(50),
            confidence_score DECIMAL(3,2),
            created_date DATE
        )
    """)

    cnx.commit()
    print("All tables created")

    # Loading data into tables

    # Load customers
    insert_customers = """
        INSERT INTO ecommerce.customers (customer_id, email, first_name, last_name, signup_date, country, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    for customer in customers:
        cursor.execute(insert_customers, (
            customer["customer_id"],
            customer["email"],
            customer["first_name"],
            customer["last_name"],
            customer["signup_date"],
            customer["country"],
            customer["is_active"]
        ))
    cnx.commit()
    print(f"Loaded {len(customers)} customers to ecommerce.customers")

    # Load products
    insert_products = """
        INSERT INTO ecommerce.products (product_id, product_name, category, price)
        VALUES (%s, %s, %s, %s)
    """
    for product in products:
        cursor.execute(insert_products, (
            product["product_id"],
            product["product_name"],
            product["category"],
            product["price"]
        ))
    cnx.commit()
    print(f"Loaded {len(products)} products to ecommerce.products")

    # Load orders
    insert_orders = """
        INSERT INTO ecommerce.orders (order_id, customer_id, order_date, status, total_amount)
        VALUES (%s, %s, %s, %s, %s)
    """
    for order in orders:
        cursor.execute(insert_orders, (
            order["order_id"],
            order["customer_id"],
            order["order_date"],
            order["status"],
            order["total_amount"]
        ))
    cnx.commit()
    print(f"Loaded {len(orders)} orders to ecommerce.orders")

    # Load order items
    insert_order_items = """
        INSERT INTO ecommerce.order_items (order_item_id, order_id, product_id, quantity, price, item_total)
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    batch_size = 1000  # Process 1000 items at a time
    for i, item in enumerate(order_items, 1):
        cursor.execute(insert_order_items, (
            item["order_item_id"],
            item["order_id"],
            item["product_id"],
            item["quantity"],
            item["price"],
            item["item_total"]
        ))
        if i % batch_size == 1000:
            cnx.commit()
            print(f"Loaded {i} out of {len(order_items)} items")
    cnx.commit()
    print(f"Loaded {len(order_items)} order items to ecommerce.order_items")

    # Load users
    insert_users = """
        INSERT INTO saas_platform.users (user_id, email, company_name, signup_date, initial_plan, country, industry, 
                                        employee_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    for user in users:
        cursor.execute(insert_users, (
            user["user_id"],
            user["email"],
            user["company_name"],
            user["signup_date"],
            user["initial_plan"],
            user["country"],
            user["industry"],
            user["employee_count"]
        ))
    cnx.commit()
    print(f"Loaded {len(users)} users to saas_platform.users")

    # Load subscriptions
    insert_subscriptions = """
        INSERT INTO saas_platform.subscriptions (subscription_id, user_id, plan_type, start_date, end_date, price, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    for subscription in subscriptions:
        cursor.execute(insert_subscriptions, (
            subscription["subscription_id"],
            subscription["user_id"],
            subscription["plan_type"],
            subscription["start_date"],
            subscription["end_date"],
            subscription["price"],
            subscription["status"]
        ))
    cnx.commit()
    print(f"Loaded {len(subscriptions)} subscriptions to saas_platform.subscriptions")

    # Load events
    insert_events = """
        INSERT INTO saas_platform.events (event_id, user_id, event_type, event_category, event_timestamp, 
                                        event_date, event_properties, platform, session_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
    """
    for i, event in enumerate(events, 1):
        # Convert event_properties dict to JSON string
        event_props_json = json.dumps(event["event_properties"]) if event["event_properties"] else '{}'
        
        cursor.execute(insert_events, (
            event["event_id"],
            event["user_id"],
            event["event_type"],
            event["event_category"],
            event["event_timestamp"],
            event["event_date"],
            event_props_json,
            event["platform"],
            event["session_id"]
        ))
        if i % batch_size == 1000:
            cnx.commit()
            print(f"Loaded {i} out of {len(events)} events")
    cnx.commit()
    print(f"Loaded {len(events)} events to saas_platform.events")

    # Load customer-user links
    insert_links = """
        INSERT INTO saas_platform.customer_user_link (link_id, customer_id, user_id, link_type, confidence_score, created_date)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    for link in links:
        cursor.execute(insert_links, (
            link["link_id"],
            link["customer_id"],
            link["user_id"],
            link["link_type"],
            link["confidence_score"],
            link["created_date"]
        ))
    cnx.commit()
    print(f"{len(links)} customer-user links to saas_platform.customer_user_link")

    # Create foreign key relationships
    cursor.execute("""
        ALTER TABLE ecommerce.orders 
        ADD CONSTRAINT fk_orders_customers 
        FOREIGN KEY (customer_id) REFERENCES ecommerce.customers(customer_id)
    """)
    
    cursor.execute("""
        ALTER TABLE ecommerce.order_items 
        ADD CONSTRAINT fk_order_items_orders 
        FOREIGN KEY (order_id) REFERENCES ecommerce.orders(order_id)
    """)
    
    cursor.execute("""
        ALTER TABLE ecommerce.order_items 
        ADD CONSTRAINT fk_order_items_products 
        FOREIGN KEY (product_id) REFERENCES ecommerce.products(product_id)
    """)
    
    cursor.execute("""
        ALTER TABLE saas_platform.subscriptions 
        ADD CONSTRAINT fk_subscriptions_users 
        FOREIGN KEY (user_id) REFERENCES saas_platform.users(user_id)
    """)

    cursor.execute("""
        ALTER TABLE saas_platform.events
        ADD CONSTRAINT fk_events_users
        FOREIGN KEY (user_id) REFERENCES saas_platform.users(user_id)
""")
    cursor.execute("""
        ALTER TABLE saas_platform.customer_user_link 
        ADD CONSTRAINT fk_link_customers 
        FOREIGN KEY (customer_id) REFERENCES ecommerce.customers(customer_id)
    """)
    
    cursor.execute("""
        ALTER TABLE saas_platform.customer_user_link 
        ADD CONSTRAINT fk_link_users 
        FOREIGN KEY (user_id) REFERENCES saas_platform.users(user_id)
    """)
    
    cnx.commit()
    print("Foreign key relationships established")

    # Close connection
    cursor.close()
    cnx.close()

# Run the ETL pipeline
if __name__ == "__main__":
    load_to_neon()



