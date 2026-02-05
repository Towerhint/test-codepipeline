import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="basic_data_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["basic", "data-pipeline"],
    description="A basic Airflow DAG for data pipeline operations",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
)
def basic_data_pipeline():
    """
    Basic data pipeline DAG with extract, transform, and load operations.
    """

    @task
    def start_pipeline():
        """
        Initialize pipeline execution and log start time.
        """
        try:
            start_time = pendulum.now("UTC")
            print(f"Pipeline started at: {start_time.to_iso8601_string()}")
            
            pipeline_info = {
                "start_time": start_time.to_iso8601_string(),
                "status": "initialized",
                "pipeline_id": "basic_data_pipeline"
            }
            
            return pipeline_info
        except Exception as e:
            print(f"Error initializing pipeline: {str(e)}")
            raise

    @task
    def extract_data(pipeline_info: dict):
        """
        Extract data from source.
        """
        try:
            print("Extracting data from source...")
            
            # Simulate data extraction
            extracted_data = {
                "pipeline_id": pipeline_info["pipeline_id"],
                "extraction_time": pendulum.now("UTC").to_iso8601_string(),
                "records": [
                    {"id": 1, "value": 100},
                    {"id": 2, "value": 200},
                    {"id": 3, "value": 300},
                ]
            }
            
            print(f"Successfully extracted {len(extracted_data['records'])} records")
            return extracted_data
        except Exception as e:
            print(f"Error during extraction: {str(e)}")
            raise

    @task
    def transform_data(extracted_data: dict):
        """
        Transform extracted data.
        """
        try:
            print("Transforming data...")
            
            records = extracted_data.get("records", [])
            
            # Apply transformation
            transformed_records = [
                {**record, "transformed_value": record["value"] * 2}
                for record in records
            ]
            
            transformed_data = {
                "pipeline_id": extracted_data["pipeline_id"],
                "transformation_time": pendulum.now("UTC").to_iso8601_string(),
                "records": transformed_records,
                "total": sum(r["transformed_value"] for r in transformed_records)
            }
            
            print(f"Transformed {len(transformed_records)} records")
            return transformed_data
        except Exception as e:
            print(f"Error during transformation: {str(e)}")
            raise

    @task
    def load_data(transformed_data: dict):
        """
        Load transformed data to destination.
        """
        try:
            print("Loading data to destination...")
            
            record_count = len(transformed_data.get("records", []))
            
            load_result = {
                "pipeline_id": transformed_data["pipeline_id"],
                "load_time": pendulum.now("UTC").to_iso8601_string(),
                "records_loaded": record_count,
                "total_value": transformed_data.get("total", 0),
                "status": "success"
            }
            
            print(f"Successfully loaded {record_count} records")
            return load_result
        except Exception as e:
            print(f"Error during load: {str(e)}")
            raise

    @task
    def complete_pipeline(load_result: dict):
        """
        Complete pipeline execution and log summary.
        """
        try:
            end_time = pendulum.now("UTC")
            
            summary = {
                "pipeline_id": load_result["pipeline_id"],
                "end_time": end_time.to_iso8601_string(),
                "records_processed": load_result["records_loaded"],
                "total_value": load_result["total_value"],
                "status": "completed"
            }
            
            print(f"Pipeline completed at: {summary['end_time']}")
            print(f"Total records processed: {summary['records_processed']}")
            
            return summary
        except Exception as e:
            print(f"Error completing pipeline: {str(e)}")
            raise

    # Define task dependencies
    pipeline_start = start_pipeline()
    extracted = extract_data(pipeline_start)
    transformed = transform_data(extracted)
    loaded = load_data(transformed)
    completed = complete_pipeline(loaded)


# Instantiate the DAG
basic_data_pipeline()
