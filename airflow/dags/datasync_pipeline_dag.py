from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys
import pandas as pd
import numpy as np
import json
from datetime import datetime

# Default arguments
default_args = {
    'owner': 'bhagyashree',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'datasync_pipeline',
    default_args=default_args,
    description='ETL pipeline with anomaly detection',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 4, 1),
    catchup=False,
)

# Define functions for each task
def extract_data(**kwargs):
    """Extract data from sample CSV file"""
    data_dir = os.path.join(os.environ.get('AIRFLOW_HOME', ''), 'data')
    os.makedirs(data_dir, exist_ok=True)
    
    # Create sample data file if it doesn't exist
    sample_file = os.path.join(data_dir, 'sales.csv')
    if not os.path.exists(sample_file):
        # Generate sample data
        df = generate_sample_data(1000)
        df.to_csv(sample_file, index=False)
    
    # Read the data
    df = pd.read_csv(sample_file)
    
    # Save to intermediate file for next task
    df.to_json(f"{data_dir}/extracted_data.json", orient="records")
    return f"Extracted {len(df)} records"

def transform_data(**kwargs):
    """Transform the extracted data"""
    data_dir = os.path.join(os.environ.get('AIRFLOW_HOME', ''), 'data')
    
    # Read the extracted data
    df = pd.read_json(f"{data_dir}/extracted_data.json", orient="records")
    
    # Apply transformations
    # 1. Drop duplicates
    df = df.drop_duplicates()
    
    # 2. Fill missing values
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(df[col].mean())
        else:
            df[col] = df[col].fillna('Unknown')
    
    # 3. Add metadata
    df['processed_at'] = datetime.now().isoformat()
    
    # Save transformed data
    df.to_json(f"{data_dir}/transformed_data.json", orient="records")
    return f"Transformed {len(df)} records"

def detect_anomalies(**kwargs):
    """Detect anomalies in the transformed data"""
    data_dir = os.path.join(os.environ.get('AIRFLOW_HOME', ''), 'data')
    
    # Read the transformed data
    df = pd.read_json(f"{data_dir}/transformed_data.json", orient="records")
    
    # Detect anomalies using z-score method
    anomalies = pd.DataFrame()
    threshold = 3.0  # z-score threshold
    
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    
    for col in numeric_cols:
        # Calculate z-score
        mean = df[col].mean()
        std = df[col].std()
        
        if std > 0:
            z_scores = (df[col] - mean) / std
            
            # Flag anomalies where z-score exceeds threshold
            col_anomalies = df[abs(z_scores) > threshold].copy()
            
            if not col_anomalies.empty:
                col_anomalies['anomaly_column'] = col
                col_anomalies['z_score'] = z_scores[abs(z_scores) > threshold]
                col_anomalies['detected_at'] = datetime.now().isoformat()
                
                anomalies = pd.concat([anomalies, col_anomalies])
    
    # Save anomalies
    if not anomalies.empty:
        anomalies.to_json(f"{data_dir}/anomalies.json", orient="records")
        return f"Detected {len(anomalies)} anomalies"
    else:
        # Create empty file if no anomalies
        with open(f"{data_dir}/anomalies.json", 'w') as f:
            f.write('[]')
        return "No anomalies detected"

def load_data(**kwargs):
    """Load the transformed data"""
    data_dir = os.path.join(os.environ.get('AIRFLOW_HOME', ''), 'data')
    
    # Read the transformed data
    df = pd.read_json(f"{data_dir}/transformed_data.json", orient="records")
    
    # In a real system, you would load to MongoDB or S3
    # For this demo, we'll save to a CSV in the output folder
    output_dir = os.path.join(data_dir, 'output')
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_dir, f"processed_data_{timestamp}.csv")
    
    df.to_csv(output_file, index=False)
    return f"Loaded data to {output_file}"

def load_to_mongodb(**kwargs):
    """Load data to MongoDB database"""
    try:
        from pymongo import MongoClient
        
        data_dir = os.path.join(os.environ.get('AIRFLOW_HOME', ''), 'data')
        
        # Read the transformed data
        df = pd.read_json(f"{data_dir}/transformed_data.json", orient="records")
        
        # Convert to dict for MongoDB
        records = df.to_dict('records')
        
        # Connect to MongoDB
        client = MongoClient('mongodb://localhost:27017/')
        db = client['datasync_db']
        collection = db['processed_data']
        
        # Insert data
        if records:
            collection.insert_many(records)
        
        return f"Loaded {len(records)} records to MongoDB"
    except Exception as e:
        return f"Error loading to MongoDB: {str(e)}"

def generate_dashboard(**kwargs):
    """Generate a dashboard of the pipeline results"""
    data_dir = os.path.join(os.environ.get('AIRFLOW_HOME', ''), 'data')
    dashboard_dir = os.path.join(data_dir, 'dashboard')
    os.makedirs(dashboard_dir, exist_ok=True)
    
    # For a real system, you would use your dashboard generator
    # Here we'll just create a simple JSON report
    
    try:
        # Load anomalies
        with open(f"{data_dir}/anomalies.json", 'r') as f:
            anomalies = json.load(f)
        anomaly_count = len(anomalies)
    except:
        anomaly_count = 0
    
    report = {
        "pipeline_run": {
            "timestamp": datetime.now().isoformat(),
            "status": "success",
            "anomalies_detected": anomaly_count
        }
    }
    
    # Save the report
    with open(os.path.join(dashboard_dir, f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"), 'w') as f:
        json.dump(report, f, indent=2)
        
    return "Dashboard generated"

def generate_sample_data(num_records=1000):
    """Generate sample sales data"""
    # Similar to your sample_data_generator.py
    from datetime import timedelta
    import random
    
    # Create date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Create customer IDs
    customer_ids = [f'CUST-{i:05d}' for i in range(1, 101)]
    
    # Create product IDs
    product_ids = [f'PROD-{i:05d}' for i in range(1, 51)]
    
    # Create regions
    regions = ['North', 'South', 'East', 'West', 'Central']
    
    # Create data dictionary
    data = {
        'transaction_id': [f'TRX-{i:06d}' for i in range(1, num_records + 1)],
        'date': [random.choice(dates).strftime('%Y-%m-%d') for _ in range(num_records)],
        'customer_id': [random.choice(customer_ids) for _ in range(num_records)],
        'product_id': [random.choice(product_ids) for _ in range(num_records)],
        'quantity': [random.randint(1, 10) for _ in range(num_records)],
        'price': [round(random.uniform(10.0, 1000.0), 2) for _ in range(num_records)],
        'region': [random.choice(regions) for _ in range(num_records)]
    }
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Calculate total
    df['total'] = df['quantity'] * df['price']
    
    # Add some missing values
    for i in range(int(num_records * 0.02)):  # 2% missing values
        idx = random.randint(0, num_records-1)
        df.loc[idx, 'region'] = None
    
    # Add anomalies (5% of records)
    for i in range(int(num_records * 0.05)):
        idx = random.randint(0, num_records-1)
        df.loc[idx, 'price'] = random.uniform(5000.0, 10000.0)
        df.loc[idx, 'total'] = df.loc[idx, 'quantity'] * df.loc[idx, 'price']
    
    return df

# Create the tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

anomaly_task = PythonOperator(
    task_id='detect_anomalies',
    python_callable=detect_anomalies,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

mongodb_task = PythonOperator(
    task_id='load_to_mongodb',
    python_callable=load_to_mongodb,
    provide_context=True,
    dag=dag,
)

dashboard_task = PythonOperator(
    task_id='generate_dashboard',
    python_callable=generate_dashboard,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> anomaly_task
anomaly_task >> [load_task, mongodb_task] >> dashboard_task
