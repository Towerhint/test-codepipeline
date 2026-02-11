"""
E2B R2 MedWatch Form Parser Pipeline
Parses FDA MedWatch forms and outputs structured data in E2B R2 XML format.
"""
import pendulum
import logging
from pathlib import Path
from typing import Dict, List, Any
from airflow.sdk import dag, task
import xml.etree.ElementTree as ET
from xml.dom import minidom

# Configure logging
logger = logging.getLogger(__name__)


@dag(
    dag_id="e2b_r2_medwatch_parser_pipeline_thinktrends",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["medwatch", "e2b-r2", "xml", "parser", "fda"],
    description="Parse FDA MedWatch forms and convert to E2B R2 XML format",
    default_args={
        "owner": "airflow",
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
        "execution_timeout": pendulum.duration(minutes=30),
    },
)
def e2b_r2_medwatch_parser_pipeline():
    """
    E2B R2 MedWatch Form Parser Pipeline
    
    This pipeline processes FDA MedWatch adverse event reports and converts them
    to the ICH E2B(R2) XML standard format for international safety reporting.
    
    Pipeline Steps:
    1. Initialize pipeline and validate configuration
    2. Scan input directory for MedWatch forms
    3. Parse each form and extract adverse event data
    4. Validate extracted data against E2B R2 requirements
    5. Generate E2B R2 XML output
    6. Save XML files and generate summary report
    """

    @task
    def initialize_pipeline():
        """
        Initialize pipeline execution and validate configuration.
        """
        try:
            start_time = pendulum.now("UTC")
            logger.info(f"E2B R2 MedWatch Parser Pipeline started at: {start_time}")
            
            # Configuration
            config = {
                "start_time": start_time.to_iso8601_string(),
                "pipeline_version": "1.0.0",
                "e2b_version": "R2",
                "input_path": "/data/input/medwatch",
                "output_path": "/data/output/e2b_xml",
                "archive_path": "/data/archive/medwatch",
                "error_path": "/data/errors/medwatch",
                "max_file_size_mb": 50,
                "batch_size": 100,
                "status": "initialized"
            }
            
            logger.info(f"Pipeline configuration: {config}")
            return config
            
        except Exception as e:
            logger.error(f"Error initializing pipeline: {str(e)}")
            raise

    @task
    def scan_input_files(config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Scan input directory for MedWatch forms to process.
        """
        try:
            logger.info(f"Scanning input directory: {config['input_path']}")
            
            # Simulate file scanning (in production, use actual file system operations)
            input_files = [
                {
                    "file_id": "medwatch_001",
                    "filename": "medwatch_report_001.txt",
                    "filepath": f"{config['input_path']}/medwatch_report_001.txt",
                    "size_kb": 125,
                    "detected_at": pendulum.now("UTC").to_iso8601_string()
                },
                {
                    "file_id": "medwatch_002",
                    "filename": "medwatch_report_002.txt",
                    "filepath": f"{config['input_path']}/medwatch_report_002.txt",
                    "size_kb": 98,
                    "detected_at": pendulum.now("UTC").to_iso8601_string()
                },
                {
                    "file_id": "medwatch_003",
                    "filename": "medwatch_report_003.txt",
                    "filepath": f"{config['input_path']}/medwatch_report_003.txt",
                    "size_kb": 156,
                    "detected_at": pendulum.now("UTC").to_iso8601_string()
                }
            ]
            
            scan_result = {
                "config": config,
                "files": input_files,
                "total_files": len(input_files),
                "scan_time": pendulum.now("UTC").to_iso8601_string()
            }
            
            logger.info(f"Found {len(input_files)} MedWatch forms to process")
            return scan_result
            
        except Exception as e:
            logger.error(f"Error scanning input files: {str(e)}")
            raise

    @task
    def parse_medwatch_forms(scan_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse MedWatch forms and extract adverse event data.
        """
        try:
            logger.info(f"Parsing {scan_result['total_files']} MedWatch forms")
            
            parsed_reports = []
            
            for file_info in scan_result['files']:
                # Simulate parsing MedWatch form data
                # In production, implement actual form parsing logic
                report_data = {
                    "file_id": file_info['file_id'],
                    "source_file": file_info['filename'],
                    "safety_report": {
                        "safetyreportid": f"US-FDA-{file_info['file_id'].upper()}-20250211",
                        "safetyreportversion": "1",
                        "receivedate": "20250211",
                        "receiptdate": "20250211",
                        "transmissiondate": "20250212",
                        "reporttype": "1",  # 1=Spontaneous report
                        "serious": "1",     # 1=Yes, 2=No
                        "seriousnessdeath": "2",
                        "seriousnesslifethreatening": "2",
                        "seriousnesshospitalization": "1",
                        "seriousnessdisabling": "2",
                        "seriousnesscongenitalanomali": "2",
                        "seriousnessother": "2"
                    },
                    "primarysource": {
                        "reportergivename": "John",
                        "reporterfamilyname": "Smith",
                        "reportercountry": "US",
                        "qualification": "1"  # 1=Physician
                    },
                    "patient": {
                        "patientinitial": "ABC",
                        "patientonsetage": "65",
                        "patientonsetageunit": "801",  # Years
                        "patientsex": "1",  # 1=Male, 2=Female
                        "patientweight": "75.5",
                        "patientweightunit": "kg"
                    },
                    "drugs": [
                        {
                            "drugcharacterization": "1",  # 1=Suspect
                            "medicinalproduct": "ASPIRIN",
                            "drugstructuredosagenumb": "100",
                            "drugstructuredosageunit": "mg",
                            "drugdosageform": "TABLET",
                            "drugadministrationroute": "048",  # Oral
                            "drugstartdate": "20250101",
                            "drugenddate": "20250210"
                        }
                    ],
                    "reactions": [
                        {
                            "reactionmeddraversionllt": "24.0",
                            "reactionmeddrallt": "Nausea",
                            "reactionmeddrapt": "Nausea",
                            "reactionoutcome": "1"  # 1=Recovered/Resolved
                        },
                        {
                            "reactionmeddraversionllt": "24.0",
                            "reactionmeddrallt": "Dizziness",
                            "reactionmeddrapt": "Dizziness",
                            "reactionoutcome": "1"
                        }
                    ],
                    "parsed_at": pendulum.now("UTC").to_iso8601_string()
                }
                
                parsed_reports.append(report_data)
                logger.info(f"Parsed report: {report_data['safety_report']['safetyreportid']}")
            
            parse_result = {
                "config": scan_result['config'],
                "parsed_reports": parsed_reports,
                "total_parsed": len(parsed_reports),
                "parse_time": pendulum.now("UTC").to_iso8601_string()
            }
            
            logger.info(f"Successfully parsed {len(parsed_reports)} reports")
            return parse_result
            
        except Exception as e:
            logger.error(f"Error parsing MedWatch forms: {str(e)}")
            raise

    @task
    def validate_e2b_data(parse_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate parsed data against E2B R2 requirements.
        """
        try:
            logger.info("Validating parsed data against E2B R2 standards")
            
            validated_reports = []
            validation_errors = []
            
            for report in parse_result['parsed_reports']:
                validation_result = {
                    "report_id": report['safety_report']['safetyreportid'],
                    "is_valid": True,
                    "errors": [],
                    "warnings": []
                }
                
                # Validate required fields
                if not report['safety_report'].get('safetyreportid'):
                    validation_result['is_valid'] = False
                    validation_result['errors'].append("Missing safety report ID")
                
                if not report['safety_report'].get('receivedate'):
                    validation_result['is_valid'] = False
                    validation_result['errors'].append("Missing receive date")
                
                if not report['drugs'] or len(report['drugs']) == 0:
                    validation_result['is_valid'] = False
                    validation_result['errors'].append("No drug information provided")
                
                if not report['reactions'] or len(report['reactions']) == 0:
                    validation_result['is_valid'] = False
                    validation_result['errors'].append("No reaction information provided")
                
                # Validate data formats
                if report['patient'].get('patientonsetage'):
                    try:
                        age = float(report['patient']['patientonsetage'])
                        if age < 0 or age > 120:
                            validation_result['warnings'].append(f"Unusual patient age: {age}")
                    except ValueError:
                        validation_result['errors'].append("Invalid patient age format")
                
                if validation_result['is_valid']:
                    validated_reports.append(report)
                    logger.info(f"Report {validation_result['report_id']} validated successfully")
                else:
                    validation_errors.append(validation_result)
                    logger.warning(f"Report {validation_result['report_id']} failed validation: {validation_result['errors']}")
            
            validation_summary = {
                "config": parse_result['config'],
                "validated_reports": validated_reports,
                "validation_errors": validation_errors,
                "total_valid": len(validated_reports),
                "total_invalid": len(validation_errors),
                "validation_time": pendulum.now("UTC").to_iso8601_string()
            }
            
            logger.info(f"Validation complete: {len(validated_reports)} valid, {len(validation_errors)} invalid")
            return validation_summary
            
        except Exception as e:
            logger.error(f"Error during validation: {str(e)}")
            raise

    @task
    def generate_e2b_xml(validation_summary: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate E2B R2 XML output from validated data.
        """
        try:
            logger.info("Generating E2B R2 XML output")
            
            generated_files = []
            
            for report in validation_summary['validated_reports']:
                # Create E2B R2 XML structure
                ichicsr = ET.Element('ichicsr', attrib={'lang': 'en'})
                
                # Message Header
                ichicsrmessageheader = ET.SubElement(ichicsr, 'ichicsrmessageheader')
                ET.SubElement(ichicsrmessageheader, 'messagetype').text = 'ichicsr'
                ET.SubElement(ichicsrmessageheader, 'messageformatversion').text = '2.1'
                ET.SubElement(ichicsrmessageheader, 'messageformatrelease').text = '2.0'
                ET.SubElement(ichicsrmessageheader, 'messagenumb').text = report['safety_report']['safetyreportid']
                ET.SubElement(ichicsrmessageheader, 'messagesenderidentifier').text = 'US-FDA'
                ET.SubElement(ichicsrmessageheader, 'messagereceiveridentifier').text = 'RECEIVER'
                ET.SubElement(ichicsrmessageheader, 'messagedateformat').text = '204'
                ET.SubElement(ichicsrmessageheader, 'messagedate').text = report['safety_report']['transmissiondate']
                
                # Safety Report
                safetyreport = ET.SubElement(ichicsr, 'safetyreport')
                
                # Safety Report ID
                ET.SubElement(safetyreport, 'safetyreportversion').text = report['safety_report']['safetyreportversion']
                ET.SubElement(safetyreport, 'safetyreportid').text = report['safety_report']['safetyreportid']
                
                # Primary Source
                primarysourcecountry = ET.SubElement(safetyreport, 'primarysourcecountry')
                primarysourcecountry.text = report['primarysource']['reportercountry']
                
                # Occurrence Country
                ET.SubElement(safetyreport, 'occurcountry').text = 'US'
                
                # Transmission Date
                ET.SubElement(safetyreport, 'transmissiondate').text = report['safety_report']['transmissiondate']
                ET.SubElement(safetyreport, 'transmissiondateformat').text = '102'
                
                # Report Type
                ET.SubElement(safetyreport, 'reporttype').text = report['safety_report']['reporttype']
                
                # Serious
                ET.SubElement(safetyreport, 'serious').text = report['safety_report']['serious']
                ET.SubElement(safetyreport, 'seriousnessdeath').text = report['safety_report']['seriousnessdeath']
                ET.SubElement(safetyreport, 'seriousnesslifethreatening').text = report['safety_report']['seriousnesslifethreatening']
                ET.SubElement(safetyreport, 'seriousnesshospitalization').text = report['safety_report']['seriousnesshospitalization']
                ET.SubElement(safetyreport, 'seriousnessdisabling').text = report['safety_report']['seriousnessdisabling']
                
                # Receive Date
                ET.SubElement(safetyreport, 'receivedate').text = report['safety_report']['receivedate']
                ET.SubElement(safetyreport, 'receivedateformat').text = '102'
                
                # Receipt Date
                ET.SubElement(safetyreport, 'receiptdate').text = report['safety_report']['receiptdate']
                ET.SubElement(safetyreport, 'receiptdateformat').text = '102'
                
                # Primary Source
                primarysource = ET.SubElement(safetyreport, 'primarysource')
                ET.SubElement(primarysource, 'reportertitle').text = ''
                ET.SubElement(primarysource, 'reportergivename').text = report['primarysource']['reportergivename']
                ET.SubElement(primarysource, 'reporterfamilyname').text = report['primarysource']['reporterfamilyname']
                ET.SubElement(primarysource, 'reportercountry').text = report['primarysource']['reportercountry']
                ET.SubElement(primarysource, 'qualification').text = report['primarysource']['qualification']
                
                # Patient
                patient = ET.SubElement(safetyreport, 'patient')
                ET.SubElement(patient, 'patientinitial').text = report['patient']['patientinitial']
                ET.SubElement(patient, 'patientonsetage').text = report['patient']['patientonsetage']
                ET.SubElement(patient, 'patientonsetageunit').text = report['patient']['patientonsetageunit']
                ET.SubElement(patient, 'patientsex').text = report['patient']['patientsex']
                ET.SubElement(patient, 'patientweight').text = report['patient']['patientweight']
                
                # Reactions
                for reaction in report['reactions']:
                    reaction_elem = ET.SubElement(patient, 'reaction')
                    ET.SubElement(reaction_elem, 'primarysourcereaction').text = reaction['reactionmeddrallt']
                    ET.SubElement(reaction_elem, 'reactionmeddraversionllt').text = reaction['reactionmeddraversionllt']
                    ET.SubElement(reaction_elem, 'reactionmeddrallt').text = reaction['reactionmeddrallt']
                    ET.SubElement(reaction_elem, 'reactionmeddrapt').text = reaction['reactionmeddrapt']
                    ET.SubElement(reaction_elem, 'reactionoutcome').text = reaction['reactionoutcome']
                
                # Drugs
                for drug in report['drugs']:
                    drug_elem = ET.SubElement(patient, 'drug')
                    ET.SubElement(drug_elem, 'drugcharacterization').text = drug['drugcharacterization']
                    ET.SubElement(drug_elem, 'medicinalproduct').text = drug['medicinalproduct']
                    
                    # Dosage
                    drugdosage = ET.SubElement(drug_elem, 'drugdosage')
                    ET.SubElement(drugdosage, 'drugdosagetext').text = f"{drug['drugstructuredosagenumb']} {drug['drugstructuredosageunit']}"
                    ET.SubElement(drugdosage, 'drugstructuredosagenumb').text = drug['drugstructuredosagenumb']
                    ET.SubElement(drugdosage, 'drugstructuredosageunit').text = drug['drugstructuredosageunit']
                    
                    ET.SubElement(drug_elem, 'drugdosageform').text = drug['drugdosageform']
                    ET.SubElement(drug_elem, 'drugadministrationroute').text = drug['drugadministrationroute']
                    ET.SubElement(drug_elem, 'drugstartdate').text = drug['drugstartdate']
                    ET.SubElement(drug_elem, 'drugstartdateformat').text = '102'
                    ET.SubElement(drug_elem, 'drugenddate').text = drug['drugenddate']
                    ET.SubElement(drug_elem, 'drugenddateformat').text = '102'
                
                # Convert to pretty XML string
                xml_string = ET.tostring(ichicsr, encoding='utf-8', method='xml')
                dom = minidom.parseString(xml_string)
                pretty_xml = dom.toprettyxml(indent='  ', encoding='utf-8').decode('utf-8')
                
                # Remove extra blank lines
                pretty_xml = '\n'.join([line for line in pretty_xml.split('\n') if line.strip()])
                
                output_filename = f"{report['safety_report']['safetyreportid']}.xml"
                output_filepath = f"{validation_summary['config']['output_path']}/{output_filename}"
                
                file_info = {
                    "report_id": report['safety_report']['safetyreportid'],
                    "source_file": report['source_file'],
                    "output_file": output_filename,
                    "output_path": output_filepath,
                    "xml_content": pretty_xml,
                    "xml_size_bytes": len(pretty_xml.encode('utf-8')),
                    "generated_at": pendulum.now("UTC").to_iso8601_string()
                }
                
                generated_files.append(file_info)
                logger.info(f"Generated E2B R2 XML: {output_filename}")
            
            generation_result = {
                "config": validation_summary['config'],
                "generated_files": generated_files,
                "validation_errors": validation_summary['validation_errors'],
                "total_generated": len(generated_files),
                "generation_time": pendulum.now("UTC").to_iso8601_string()
            }
            
            logger.info(f"Successfully generated {len(generated_files)} E2B R2 XML files")
            return generation_result
            
        except Exception as e:
            logger.error(f"Error generating E2B R2 XML: {str(e)}")
            raise

    @task
    def save_output_files(generation_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Save generated XML files to output directory.
        """
        try:
            logger.info("Saving XML output files")
            
            saved_files = []
            
            for file_info in generation_result['generated_files']:
                # In production, write actual files to disk
                # For this implementation, we simulate the save operation
                
                save_info = {
                    "report_id": file_info['report_id'],
                    "output_file": file_info['output_file'],
                    "output_path": file_info['output_path'],
                    "size_bytes": file_info['xml_size_bytes'],
                    "saved_at": pendulum.now("UTC").to_iso8601_string(),
                    "status": "success"
                }
                
                saved_files.append(save_info)
                logger.info(f"Saved XML file: {save_info['output_file']} ({save_info['size_bytes']} bytes)")
            
            save_result = {
                "config": generation_result['config'],
                "saved_files": saved_files,
                "validation_errors": generation_result['validation_errors'],
                "total_saved": len(saved_files),
                "save_time": pendulum.now("UTC").to_iso8601_string()
            }
            
            logger.info(f"Successfully saved {len(saved_files)} XML files")
            return save_result
            
        except Exception as e:
            logger.error(f"Error saving output files: {str(e)}")
            raise

    @task
    def generate_summary_report(save_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate pipeline execution summary report.
        """
        try:
            logger.info("Generating pipeline summary report")
            
            end_time = pendulum.now("UTC")
            start_time = pendulum.parse(save_result['config']['start_time'])
            duration = end_time.diff(start_time)
            
            summary = {
                "pipeline_id": "e2b_r2_medwatch_parser_pipeline",
                "pipeline_version": save_result['config']['pipeline_version'],
                "e2b_version": save_result['config']['e2b_version'],
                "execution": {
                    "start_time": save_result['config']['start_time'],
                    "end_time": end_time.to_iso8601_string(),
                    "duration_seconds": duration.in_seconds(),
                    "status": "completed"
                },
                "processing_summary": {
                    "total_files_processed": save_result['total_saved'] + len(save_result['validation_errors']),
                    "successful_conversions": save_result['total_saved'],
                    "failed_validations": len(save_result['validation_errors']),
                    "success_rate": round(save_result['total_saved'] / (save_result['total_saved'] + len(save_result['validation_errors'])) * 100, 2) if (save_result['total_saved'] + len(save_result['validation_errors'])) > 0 else 0
                },
                "output_files": save_result['saved_files'],
                "validation_errors": save_result['validation_errors'],
                "paths": {
                    "input": save_result['config']['input_path'],
                    "output": save_result['config']['output_path'],
                    "archive": save_result['config']['archive_path'],
                    "error": save_result['config']['error_path']
                }
            }
            
            logger.info("=" * 80)
            logger.info("E2B R2 MEDWATCH PARSER PIPELINE - EXECUTION SUMMARY")
            logger.info("=" * 80)
            logger.info(f"Pipeline Version: {summary['pipeline_version']}")
            logger.info(f"E2B Version: {summary['e2b_version']}")
            logger.info(f"Start Time: {summary['execution']['start_time']}")
            logger.info(f"End Time: {summary['execution']['end_time']}")
            logger.info(f"Duration: {summary['execution']['duration_seconds']} seconds")
            logger.info(f"Status: {summary['execution']['status']}")
            logger.info("-" * 80)
            logger.info(f"Total Files Processed: {summary['processing_summary']['total_files_processed']}")
            logger.info(f"Successful Conversions: {summary['processing_summary']['successful_conversions']}")
            logger.info(f"Failed Validations: {summary['processing_summary']['failed_validations']}")
            logger.info(f"Success Rate: {summary['processing_summary']['success_rate']}%")
            logger.info("=" * 80)
            
            return summary
            
        except Exception as e:
            logger.error(f"Error generating summary report: {str(e)}")
            raise

    # Define task dependencies
    config = initialize_pipeline()
    scan_result = scan_input_files(config)
    parse_result = parse_medwatch_forms(scan_result)
    validation_summary = validate_e2b_data(parse_result)
    generation_result = generate_e2b_xml(validation_summary)
    save_result = save_output_files(generation_result)
    summary = generate_summary_report(save_result)


# Instantiate the DAG
e2b_r2_medwatch_parser_pipeline()
