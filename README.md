# Modern Data Stack Analytics Pipeline: E-commerce & SaaS Data
## Project Overview
This project demonstrates an end-to-end analytics pipeline that transforms raw e-commerce and SaaS data into an analytics-ready Kimball star schema. I simulated realistic business data, built a multi-layered dbt transformation pipeline, and deployed using dbt Cloud, all connected to a Neon PostgreSQL database.

## Pipeline Components:
Data Simulation: Python scripts generate realistic e-commerce (customers, orders, products) and SaaS (users, events, subscriptions) data

Data Storage: Neon PostgreSQL database with separate schemas for raw and transformed data

Transformation: dbt models organized in staging, marts and analytics layers

Orchestration: dbt Cloud with Git-based CI/CD workflow

## Technologies Used
1. Python:	Data simulation and database loading
2. Neon PostgreSQL:	Cloud database for raw and transformed data
4. dbt Cloud:	Data transformation and modeling, CI/CD, job deployment
5. Git/GitHub:	Version control and collaboration

## Data Model
### Source Data (Raw)
1. E-commerce: customers, products, orders, order_items
2. SaaS Platform: users, subscriptions, events, customer_user_link

### Transformed Layers:
1. Staging: Cleaned views of raw data with standardized naming
  
2. Marts: Kimball-style star schema:
Dimensions: dim_customers, dim_products, dim_users
Facts: fct_orders, fct_events, fct_subscriptions fct_customer_user_link

3. Analytics: Aggregated business metrics tables

## Development Workflow
1. Local Development: Write Python scripts and dbt models
2. Version Control: Commit changes using Git with descriptive messages
3. Code Review: Create Pull Requests in dbt Cloud
4. Testing: dbt Cloud automatically tests changes on PRs
5. Deployment: Merge to main, run production job
6. Monitoring: Production job runs successfully, updating tables

## Prerequisites
1. Python 3.8+
2. dbt Cloud account
3. Neon PostgreSQL database
4. Git

## Installation Steps
 **1. Clone the repository:**
   ```bash
   git clone https://github.com/Oreoluwa100/business-intelligence-pipeline.git
   cd business-intelligence-pipeline
   ```

**2. Set up environment variables:**


Create a `.env` file with your Neon database credentials:

```bash
NEON_HOST=your_host
NEON_USER=your_user
NEON_PASSWORD=your_password
NEON_DATABASE=your_database
NEON_PORT=5432
```

**3. Generate and load data:**
```
python load_data_to_db.py
```

**4. Connect dbt Cloud to your Neon database**

**5. Run dbt models:**
```bash
dbt build
dbt docs generate
```

## Results
* Successfully built 19 dbt models across three layers
* Created unified customer view connecting e-commerce and SaaS data
* Implemented data quality tests on all source tables
* Established CI/CD pipeline with dbt Cloud and GitHub
* Deployed production job that runs successfully

## Key Features
* Realistic Data Simulation: Python scripts generate business-realistic data with proper relationships
* Modern Data Stack: Uses industry standard tools (dbt, PostgreSQL, Git)
* Production Ready: Includes data quality tests, incremental models, and deployment job
* Cross-Domain Analytics: Connects e-commerce and SaaS data for comprehensive insights

## Skills Demonstrated
* Data Engineering: End-to-end pipeline development from simulation to analytics
* dbt Modeling: Building layered data models with tests and documentation
* Cloud Databases: Working with Neon PostgreSQL
* CI/CD: Implementing professional deployment workflows with dbt Cloud
* Version Control: Using Git for collaborative development






