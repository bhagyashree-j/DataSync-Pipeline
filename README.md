# DataSync Pipeline

A robust ETL solution with automated monitoring and anomaly detection for seamless data transfers between complex systems.

## Features

- **Flexible Data Extraction**: Extract data from various sources including CSV files and MongoDB collections
- **Data Transformation**: Apply transformations to clean and prepare data
- **Anomaly Detection**: Automatically detect anomalies in the data using statistical methods
- **Data Loading**: Load processed data to multiple destinations, including MongoDB and CSV files
- **Pipeline Monitoring**: Monitor the ETL process with detailed logging
- **Automated Workflow**: Schedule and orchestrate the ETL process using Apache Airflow
- **Dashboard Generation**: Generate pipeline performance reports

## Tech Stack

- Python
- Apache Airflow
- MongoDB
- Pandas & NumPy
- Data Visualization Tools

## Project Structure

DataSync-Pipeline/

├── airflow/

│   ├── dags/    

│   │   └── datasync_pipeline_dag.py 

│   └── data/            

├── airflow_venv/         

└── README.md            

## Setup and Usage

### Prerequisites
- Python 3.x
- MongoDB
- Apache Airflow

### Installation
1. Clone the repository
2. Create a virtual environment: `python -m venv airflow_venv`
3. Activate the environment: `source airflow_venv/bin/activate`
4. Install dependencies: `pip install apache-airflow pandas numpy matplotlib pymongo`
5. Initialize Airflow database: `airflow db init`
6. Create Airflow admin user
7. Start Airflow webserver and scheduler

### Running the Pipeline
1. Access Airflow UI at http://localhost:8080
2. Enable the DataSync Pipeline DAG
3. Trigger the DAG to run the pipeline

## License

MIT
