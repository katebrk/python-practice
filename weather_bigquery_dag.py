from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import requests
import pandas as pd
import time
import logging
from typing import List, Dict, Any
import json
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import io

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration - you can also use Airflow Variables/Connections for this
CONFIG = {
    'project_id': 'uk-cities-weather',
    'credentials_path': '/opt/airflow/config/uk-cities-weather-9372a013498e.json',  # Path inside container
    'table_id': 'uk-cities-weather.weather_dataset.hourly_weather',
    'location': 'EU'
}

class WeatherDataPipeline:
    """Weather data pipeline adapted for Airflow"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.cities = {
            'London': {'lat': 51.5074, 'lon': -0.1278},
            'Manchester': {'lat': 53.4808, 'lon': -2.2426},
            'Liverpool': {'lat': 53.4084, 'lon': -2.9916},
            'Edinburgh': {'lat': 55.9533, 'lon': -3.1883}
        }
        self.base_url = "https://api.open-meteo.com/v1/forecast"

        # Define BigQuery schema to avoid autodetect issues
        self.bq_schema = [
            bigquery.SchemaField("city", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("temperature_celsius", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("humidity_percent", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("precipitation_mm", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("wind_speed_kmh", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("wind_direction_degrees", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("pressure_hpa", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("extracted_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("hour", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("day_of_week", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("month", "INTEGER", mode="REQUIRED"),
        ]

    def extract_weather_data(self, days_back: int = 7) -> List[Dict]:
        """Extract weather data from Open-Meteo API"""
        all_weather_data = []
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back)

        for city_name, coordinates in self.cities.items():
            try:
                logger.info(f"Extracting weather data for {city_name}")
                
                params = {
                    'latitude': coordinates['lat'],
                    'longitude': coordinates['lon'],
                    'start_date': start_date.isoformat(),
                    'end_date': end_date.isoformat(),
                    'hourly': [
                        'temperature_2m',
                        'relative_humidity_2m',
                        'precipitation',
                        'wind_speed_10m',
                        'wind_direction_10m',
                        'pressure_msl'
                    ],
                    'timezone': 'Europe/London'
                }

                response = requests.get(self.base_url, params=params)
                response.raise_for_status()
                data = response.json()
                
                hourly_data = self._process_hourly_data(data, city_name)
                all_weather_data.extend(hourly_data)
                time.sleep(1)  # Be respectful to API

            except Exception as e:
                logger.error(f"Error extracting data for {city_name}: {str(e)}")
                continue

        logger.info(f"Successfully extracted {len(all_weather_data)} weather records")
        return all_weather_data

    def _process_hourly_data(self, api_data: Dict, city_name: str) -> List[Dict]:
        """Process hourly weather data from API response"""
        hourly_data = []
        hourly = api_data.get('hourly', {})
        
        if not hourly:
            return hourly_data

        times = hourly.get('time', [])
        
        for i, timestamp in enumerate(times):
            record = {
                'city': city_name,
                'timestamp': timestamp,
                'temperature_celsius': hourly.get('temperature_2m', [None])[i],
                'humidity_percent': hourly.get('relative_humidity_2m', [None])[i],
                'precipitation_mm': hourly.get('precipitation', [None])[i],
                'wind_speed_kmh': hourly.get('wind_speed_10m', [None])[i],
                'wind_direction_degrees': hourly.get('wind_direction_10m', [None])[i],
                'pressure_hpa': hourly.get('pressure_msl', [None])[i],
                'extracted_at': datetime.now().isoformat()
            }
            hourly_data.append(record)
        
        return hourly_data

    def transform_data(self, raw_data: List[Dict]) -> pd.DataFrame:
        """Transform raw weather data with proper data type handling"""
        logger.info("Starting data transformation")
    
        df = pd.DataFrame(raw_data)
        if df.empty:
            logger.warning("No data to transform")
            return df
    
        # Convert timestamp columns properly
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        df['extracted_at'] = pd.to_datetime(df['extracted_at'], utc=True)
    
        # Add date features
        df['date'] = df['timestamp'].dt.date
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        df['month'] = df['timestamp'].dt.month
    
        # Basic data cleaning
        df = df.dropna(subset=['temperature_celsius'])
        df = df.drop_duplicates(subset=['city', 'timestamp'])
    
        # Convert numeric columns to proper types
        numeric_columns = [
            'temperature_celsius', 'humidity_percent', 'precipitation_mm',
            'wind_speed_kmh', 'wind_direction_degrees', 'pressure_hpa'
        ]
        
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Convert integer columns
        integer_columns = ['hour', 'day_of_week', 'month']
        for col in integer_columns:
            df[col] = df[col].astype('int64')
        
        # Handle infinite values
        df = df.replace([float('inf'), -float('inf'), float('nan')], None)
    
        logger.info(f"Transformation complete. Dataset shape: {df.shape}")
        logger.info(f"Data types: {df.dtypes}")
        return df

    def load_to_bigquery(self, df: pd.DataFrame) -> None:
        """Load data to BigQuery with proper schema and data type handling"""
        try:
            logger.info("Loading data to BigQuery")
            logger.info(f"DataFrame shape: {df.shape}")
            
            if df.empty:
                logger.warning("No data to load")
                return
            
            # Initialize BigQuery client
            if 'credentials_path' in self.config:
                credentials_path = self.config['credentials_path']
                logger.info(f"Using credentials from: {credentials_path}")
                
                if not os.path.exists(credentials_path):
                    raise FileNotFoundError(f"Credentials file not found: {credentials_path}")
                    
                credentials = service_account.Credentials.from_service_account_file(
                    credentials_path
                )
                client = bigquery.Client(
                    credentials=credentials, 
                    project=self.config['project_id']
                )
            else:
                logger.info("Using default credentials")
                client = bigquery.Client(project=self.config['project_id'])
                
            table_id = self.config['table_id']
            logger.info(f"Target table: {table_id}")
            
            # Create a copy of the dataframe for BigQuery loading
            df_bq = df.copy()
            
            # Format datetime columns for BigQuery
            df_bq['timestamp'] = df_bq['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S UTC')
            df_bq['extracted_at'] = df_bq['extracted_at'].dt.strftime('%Y-%m-%d %H:%M:%S UTC')
            df_bq['date'] = df_bq['date'].astype(str)
            
            # Ensure all numeric columns are properly formatted
            numeric_columns = [
                'temperature_celsius', 'humidity_percent', 'precipitation_mm',
                'wind_speed_kmh', 'wind_direction_degrees', 'pressure_hpa'
            ]
            
            for col in numeric_columns:
                # Convert NaN to None for BigQuery compatibility
                df_bq[col] = df_bq[col].where(pd.notnull(df_bq[col]), None)
            
            # Configure load job with explicit schema
            job_config = bigquery.LoadJobConfig(
                schema=self.bq_schema,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,  # Skip header row
                allow_quoted_newlines=True,
                allow_jagged_rows=False,
                max_bad_records=0
            )
            
            # Convert DataFrame to CSV
            logger.info("Converting DataFrame to CSV format...")
            csv_buffer = df_bq.to_csv(index=False, na_rep='')
            
            # Load data using CSV method
            logger.info("Starting BigQuery load job...")
            job = client.load_table_from_file(
                io.StringIO(csv_buffer), 
                table_id, 
                job_config=job_config
            )
            
            # Wait for completion and handle errors
            job.result()
            
            if job.errors:
                logger.error(f"Load job completed with errors: {job.errors}")
                raise Exception(f"BigQuery load failed with errors: {job.errors}")
            
            logger.info(f"Successfully loaded {len(df)} rows to BigQuery")
            
            # Log job statistics
            if job.output_rows:
                logger.info(f"Job statistics - Output rows: {job.output_rows}")
            
        except Exception as e:
            logger.error(f"Error loading to BigQuery: {str(e)}")
            logger.error(f"Error type: {type(e)}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            raise

    def serialize_for_xcom(self, df: pd.DataFrame) -> Dict:
        """Serialize DataFrame for XCom with proper data type handling"""
        # Convert DataFrame to records, handling datetime and other complex types
        df_copy = df.copy()
        
        # Convert datetime columns to ISO format strings
        datetime_columns = df_copy.select_dtypes(include=['datetime64[ns, UTC]', 'datetime64[ns]']).columns
        for col in datetime_columns:
            df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d %H:%M:%S UTC')
        
        # Convert date columns to string
        if 'date' in df_copy.columns:
            df_copy['date'] = df_copy['date'].astype(str)
        
        # Handle NaN values
        df_copy = df_copy.where(pd.notnull(df_copy), None)
        
        return {
            'records': df_copy.to_dict('records'),
            'dtypes': df_copy.dtypes.astype(str).to_dict()
        }

    def deserialize_from_xcom(self, xcom_data: Dict) -> pd.DataFrame:
        """Deserialize DataFrame from XCom data"""
        if not xcom_data or 'records' not in xcom_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(xcom_data['records'])
        
        if df.empty:
            return df
        
        # Restore datetime columns
        datetime_columns = ['timestamp', 'extracted_at']
        for col in datetime_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], utc=True)
        
        # Restore date column
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.date
        
        # Restore numeric columns
        numeric_columns = [
            'temperature_celsius', 'humidity_percent', 'precipitation_mm',
            'wind_speed_kmh', 'wind_direction_degrees', 'pressure_hpa'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df


# Airflow task functions with improved error handling
def extract_weather_data_task(**context):
    """Airflow task for data extraction"""
    try:
        pipeline = WeatherDataPipeline(CONFIG)
        data = pipeline.extract_weather_data(days_back=7)
        
        if not data:
            raise ValueError("No weather data extracted")
        
        logger.info(f"Extracted {len(data)} records")
        return data
        
    except Exception as e:
        logger.error(f"Extract task failed: {str(e)}")
        raise

def transform_data_task(**context):
    """Airflow task for data transformation"""
    try:
        # Get data from previous task
        raw_data = context['task_instance'].xcom_pull(task_ids='extract_weather_data')
        
        if not raw_data:
            raise ValueError("No data received from extract task")
        
        pipeline = WeatherDataPipeline(CONFIG)
        df = pipeline.transform_data(raw_data)
        
        if df.empty:
            raise ValueError("No data after transformation")
        
        # Serialize DataFrame for XCom
        serialized_data = pipeline.serialize_for_xcom(df)
        logger.info(f"Transformed {len(df)} records")
        
        return serialized_data
        
    except Exception as e:
        logger.error(f"Transform task failed: {str(e)}")
        raise

def load_data_task(**context):
    """Airflow task for loading data to BigQuery"""
    try:
        # Get transformed data from previous task
        xcom_data = context['task_instance'].xcom_pull(task_ids='transform_data')
        
        if not xcom_data:
            raise ValueError("No data received from transform task")
        
        pipeline = WeatherDataPipeline(CONFIG)
        df = pipeline.deserialize_from_xcom(xcom_data)
        
        if df.empty:
            raise ValueError("No data to load")
        
        pipeline.load_to_bigquery(df)
        logger.info(f"Successfully loaded {len(df)} records to BigQuery")
        
    except Exception as e:
        logger.error(f"Load task failed: {str(e)}")
        raise

def data_quality_check(**context):
    """Enhanced data quality check"""
    try:
        xcom_data = context['task_instance'].xcom_pull(task_ids='transform_data')
        
        if not xcom_data or 'records' not in xcom_data:
            raise ValueError("No data found for quality check")
        
        records = xcom_data['records']
        df = pd.DataFrame(records)
        
        # Basic quality checks
        if df.empty:
            raise ValueError("No data found after transformation!")
        
        # Check for required columns
        required_columns = ['city', 'timestamp', 'temperature_celsius']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Check temperature data quality
        temp_null_ratio = df['temperature_celsius'].isnull().sum() / len(df)
        if temp_null_ratio > 0.5:
            raise ValueError(f"Too many missing temperature values: {temp_null_ratio:.2%}")
        
        # Check for duplicate records
        duplicates = df.duplicated(subset=['city', 'timestamp']).sum()
        if duplicates > 0:
            logger.warning(f"Found {duplicates} duplicate records")
        
        # Check data freshness (should have recent data)
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            latest_timestamp = df['timestamp'].max()
            hours_old = (datetime.now(tz=pd.Timestamp.now().tz) - latest_timestamp).total_seconds() / 3600
            if hours_old > 48:  # Data is more than 48 hours old
                logger.warning(f"Data may be stale. Latest timestamp: {latest_timestamp}")
        
        logger.info(f"Data quality check passed. {len(df)} records ready for loading.")
        logger.info(f"Temperature data completeness: {(1-temp_null_ratio):.2%}")
        
    except Exception as e:
        logger.error(f"Data quality check failed: {str(e)}")
        raise

# Define default arguments
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'weather_data_bigquery_pipeline',
    default_args=default_args,
    description='Weather data ETL pipeline to BigQuery',
    schedule_interval='@daily',  # Run daily
    catchup=False,
    max_active_runs=1,
    tags=['weather', 'bigquery', 'etl'],
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract_weather_data',
    python_callable=extract_weather_data_task,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data_task,
    dag=dag,
)

#quality_check_task = PythonOperator(
#    task_id='data_quality_check',
#    python_callable=data_quality_check,
#    dag=dag,
#)

load_task = PythonOperator(
    task_id='load_to_bigquery',
    python_callable=load_data_task,
    dag=dag,
)

# Add a simple notification task
notify_success = BashOperator(
    task_id='notify_success',
    bash_command='echo "Weather data pipeline completed successfully at $(date)"',
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task >> notify_success