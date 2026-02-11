"""
E2B R2 MedWatch Form Parser Pipeline
Processes FDA MedWatch E2B(R2) XML forms for adverse event reporting.
"""

import pendulum
from airflow.sdk import dag, task
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


@dag(
    dag_id="e2b_r2_medwatch_parser",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["medwatch", "e2b", "fda", "adverse-events"],
    description="Parse and validate FDA MedWatch E2B R2 XML forms for adverse event reporting",
    default_args={
        "owner": "airflow",
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
        "execution_timeout": pendulum.duration(minutes=30),
    },
)
def e2b_r2_medwatch_parser():
    """
    Pipeline for processing FDA MedWatch E2B(R2) adverse event forms.
    
    Workflow:
    1. Scan for new E2B R2 XML files
    2. Parse XML and extract structured data
    3. Validate against E2B R2 schema
    4. Extract patient, drug, and adverse event information
    5. Transform to standardized format
    6. Load to data warehouse
    7. Generate summary report
    """

    @task
    def initialize_pipeline():
        """
        Initialize pipeline execution and prepare environment.
        
        Returns:
            dict: Pipeline initialization metadata
        """
        try:
            start_time = pendulum.now("UTC")
            logger.info(f"Starting E2B R2 MedWatch parser at {start_time}")
            
            return {
                "pipeline_id": "e2b_r2_medwatch_parser",
                "start_time": start_time.to_iso8601_string(),
                "status": "initialized",
                "version": "1.0.0"
            }
        except Exception as e:
            logger.error(f"Pipeline initialization failed: {str(e)}")
            raise

    @task
    def scan_xml_files(init_data: dict):
        """
        Scan input directory for new E2B R2 XML files.
        
        Args:
            init_data: Pipeline initialization data
            
        Returns:
            dict: List of XML files to process
        """
        from src.tasks.form_parser import scan_for_files
        
        try:
            logger.info("Scanning for E2B R2 XML files...")
            
            files = scan_for_files()
            
            return {
                "pipeline_id": init_data["pipeline_id"],
                "scan_time": pendulum.now("UTC").to_iso8601_string(),
                "files": files,
                "file_count": len(files)
            }
        except Exception as e:
            logger.error(f"File scan failed: {str(e)}")
            raise

    @task
    def parse_e2b_forms(scan_data: dict):
        """
        Parse E2B R2 XML forms and extract structured data.
        
        Args:
            scan_data: File scan results
            
        Returns:
            dict: Parsed form data
        """
        from src.tasks.form_parser import parse_e2b_xml
        
        try:
            logger.info(f"Parsing {scan_data['file_count']} E2B R2 forms...")
            
            parsed_forms = []
            errors = []
            
            for file_path in scan_data["files"]:
                try:
                    form_data = parse_e2b_xml(file_path)
                    parsed_forms.append(form_data)
                except Exception as e:
                    logger.error(f"Failed to parse {file_path}: {str(e)}")
                    errors.append({"file": file_path, "error": str(e)})
            
            return {
                "pipeline_id": scan_data["pipeline_id"],
                "parse_time": pendulum.now("UTC").to_iso8601_string(),
                "parsed_forms": parsed_forms,
                "successful_count": len(parsed_forms),
                "error_count": len(errors),
                "errors": errors
            }
        except Exception as e:
            logger.error(f"Form parsing failed: {str(e)}")
            raise

    @task
    def validate_forms(parse_data: dict):
        """
        Validate parsed forms against E2B R2 schema and business rules.
        
        Args:
            parse_data: Parsed form data
            
        Returns:
            dict: Validation results
        """
        from src.tasks.data_validator import validate_e2b_data
        
        try:
            logger.info(f"Validating {parse_data['successful_count']} forms...")
            
            validation_results = []
            valid_forms = []
            invalid_forms = []
            
            for form in parse_data["parsed_forms"]:
                result = validate_e2b_data(form)
                validation_results.append(result)
                
                if result["is_valid"]:
                    valid_forms.append(form)
                else:
                    invalid_forms.append({
                        "form": form,
                        "validation_errors": result["errors"]
                    })
            
            return {
                "pipeline_id": parse_data["pipeline_id"],
                "validation_time": pendulum.now("UTC").to_iso8601_string(),
                "valid_forms": valid_forms,
                "invalid_forms": invalid_forms,
                "valid_count": len(valid_forms),
                "invalid_count": len(invalid_forms),
                "validation_results": validation_results
            }
        except Exception as e:
            logger.error(f"Form validation failed: {str(e)}")
            raise

    @task
    def extract_adverse_events(validation_data: dict):
        """
        Extract patient, drug, and adverse event information.
        
        Args:
            validation_data: Validation results
            
        Returns:
            dict: Extracted adverse event data
        """
        from src.tasks.form_parser import extract_adverse_event_data
        
        try:
            logger.info(f"Extracting data from {validation_data['valid_count']} valid forms...")
            
            adverse_events = []
            
            for form in validation_data["valid_forms"]:
                event_data = extract_adverse_event_data(form)
                adverse_events.append(event_data)
            
            return {
                "pipeline_id": validation_data["pipeline_id"],
                "extraction_time": pendulum.now("UTC").to_iso8601_string(),
                "adverse_events": adverse_events,
                "event_count": len(adverse_events)
            }
        except Exception as e:
            logger.error(f"Data extraction failed: {str(e)}")
            raise

    @task
    def transform_to_standard_format(extraction_data: dict):
        """
        Transform extracted data to standardized internal format.
        
        Args:
            extraction_data: Extracted adverse event data
            
        Returns:
            dict: Transformed data
        """
        from src.tasks.form_parser import transform_to_standard
        
        try:
            logger.info(f"Transforming {extraction_data['event_count']} events...")
            
            transformed_events = []
            
            for event in extraction_data["adverse_events"]:
                transformed = transform_to_standard(event)
                transformed_events.append(transformed)
            
            # Calculate aggregated statistics
            total_patients = len(set(e.get("patient_id") for e in transformed_events))
            total_drugs = sum(len(e.get("drugs", [])) for e in transformed_events)
            
            return {
                "pipeline_id": extraction_data["pipeline_id"],
                "transformation_time": pendulum.now("UTC").to_iso8601_string(),
                "transformed_events": transformed_events,
                "total_events": len(transformed_events),
                "total_patients": total_patients,
                "total_drugs": total_drugs
            }
        except Exception as e:
            logger.error(f"Data transformation failed: {str(e)}")
            raise

    @task
    def load_to_warehouse(transform_data: dict):
        """
        Load transformed data to data warehouse.
        
        Args:
            transform_data: Transformed event data
            
        Returns:
            dict: Load results
        """
        try:
            logger.info(f"Loading {transform_data['total_events']} events to warehouse...")
            
            # Simulate loading to warehouse
            # In production, this would connect to your data warehouse
            loaded_events = []
            
            for event in transform_data["transformed_events"]:
                # Add load metadata
                event["loaded_at"] = pendulum.now("UTC").to_iso8601_string()
                event["pipeline_run_id"] = transform_data["pipeline_id"]
                loaded_events.append(event)
            
            return {
                "pipeline_id": transform_data["pipeline_id"],
                "load_time": pendulum.now("UTC").to_iso8601_string(),
                "loaded_events": loaded_events,
                "records_loaded": len(loaded_events),
                "status": "success"
            }
        except Exception as e:
            logger.error(f"Data warehouse load failed: {str(e)}")
            raise

    @task
    def generate_summary_report(load_data: dict):
        """
        Generate summary report of pipeline execution.
        
        Args:
            load_data: Load results
            
        Returns:
            dict: Pipeline summary
        """
        try:
            end_time = pendulum.now("UTC")
            
            # Generate summary statistics
            summary = {
                "pipeline_id": load_data["pipeline_id"],
                "end_time": end_time.to_iso8601_string(),
                "status": "completed",
                "records_processed": load_data["records_loaded"],
                "report": {
                    "total_forms_processed": load_data["records_loaded"],
                    "successful_loads": load_data["records_loaded"],
                    "completion_timestamp": end_time.to_iso8601_string()
                }
            }
            
            logger.info(f"Pipeline completed successfully")
            logger.info(f"Total records processed: {summary['records_processed']}")
            
            return summary
        except Exception as e:
            logger.error(f"Summary report generation failed: {str(e)}")
            raise

    # Define task dependencies
    init = initialize_pipeline()
    scanned = scan_xml_files(init)
    parsed = parse_e2b_forms(scanned)
    validated = validate_forms(parsed)
    extracted = extract_adverse_events(validated)
    transformed = transform_to_standard_format(extracted)
    loaded = load_to_warehouse(transformed)
    summary = generate_summary_report(loaded)


# Instantiate the DAG
e2b_r2_medwatch_parser()
