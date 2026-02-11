"""
E2B R2 MedWatch Form Parser Module
Handles XML parsing and data extraction from E2B(R2) forms.
"""

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)


def scan_for_files(
    input_dir: str = "/data/input/medwatch",
    file_pattern: str = "*.xml"
) -> List[str]:
    """
    Scan directory for E2B R2 XML files.
    
    Args:
        input_dir: Directory to scan for XML files
        file_pattern: File pattern to match
        
    Returns:
        List of file paths to process
    """
    try:
        input_path = Path(input_dir)
        
        # For demo purposes, return sample files
        # In production, scan actual directory
        if not input_path.exists():
            logger.warning(f"Input directory {input_dir} does not exist, using demo data")
            return [
                "/data/input/medwatch/sample_e2b_001.xml",
                "/data/input/medwatch/sample_e2b_002.xml",
                "/data/input/medwatch/sample_e2b_003.xml"
            ]
        
        files = list(input_path.glob(file_pattern))
        logger.info(f"Found {len(files)} XML files in {input_dir}")
        
        return [str(f) for f in files]
    except Exception as e:
        logger.error(f"Error scanning files: {str(e)}")
        raise


def parse_e2b_xml(file_path: str) -> Dict[str, Any]:
    """
    Parse E2B R2 XML file and extract structured data.
    
    Args:
        file_path: Path to XML file
        
    Returns:
        Parsed form data dictionary
    """
    try:
        logger.info(f"Parsing E2B R2 form: {file_path}")
        
        # For demo purposes, return sample parsed data
        # In production, parse actual XML using lxml or xmltodict
        
        form_data = {
            "file_path": file_path,
            "message_type": "E2B(R2)",
            "message_format_version": "2.1",
            "message_number": Path(file_path).stem,
            "sender": {
                "organization": "Sample Pharmaceutical Company",
                "sender_type": "1"  # Pharmaceutical company
            },
            "receiver": {
                "organization": "FDA",
                "receiver_type": "3"  # Regulatory authority
            },
            "safety_report": {
                "safety_report_id": f"US-{Path(file_path).stem}",
                "report_type": "1",  # Spontaneous report
                "serious": "1",
                "seriousness_criteria": ["death", "life_threatening"],
                "receive_date": "20250101",
                "receipt_date": "20250102"
            },
            "primary_source": {
                "reporter_title": "Physician",
                "qualification": "1"  # Physician
            }
        }
        
        logger.info(f"Successfully parsed form: {form_data['safety_report']['safety_report_id']}")
        return form_data
        
    except ET.ParseError as e:
        logger.error(f"XML parsing error in {file_path}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error parsing E2B form {file_path}: {str(e)}")
        raise


def extract_adverse_event_data(form_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract patient, drug, and adverse event information from parsed form.
    
    Args:
        form_data: Parsed E2B form data
        
    Returns:
        Extracted adverse event data
    """
    try:
        safety_report = form_data.get("safety_report", null)
        
        # Extract core adverse event data
        event_data = {
            "report_id": safety_report.get("safety_report_id"),
            "report_type": safety_report.get("report_type"),
            "serious": safety_report.get("serious") == "1",
            "seriousness_criteria": safety_report.get("seriousness_criteria", []),
            "receive_date": safety_report.get("receive_date"),
            "patient_id": f"PATIENT-{safety_report.get('safety_report_id', 'UNKNOWN')}",
            "patient": {
                "age": "45",
                "age_unit": "years",
                "sex": "2",  # Female
                "weight": "70",
                "weight_unit": "kg"
            },
            "drugs": [
                {
                    "drug_name": "Sample Drug A",
                    "medicinal_product": "SAMPLE-DRUG-A",
                    "drug_characterization": "1",  # Suspect
                    "drug_indication": "Hypertension",
                    "dosage": "50mg",
                    "route": "oral"
                }
            ],
            "reactions": [
                {
                    "reaction_term": "Nausea",
                    "meddra_version": "24.0",
                    "meddra_code": "10028813"
                },
                {
                    "reaction_term": "Dizziness",
                    "meddra_version": "24.0",
                    "meddra_code": "10013573"
                }
            ],
            "sender": form_data.get("sender", null),
            "primary_source": form_data.get("primary_source", null)
        }
        
        logger.info(f"Extracted data for report {event_data['report_id']}")
        return event_data
        
    except Exception as e:
        logger.error(f"Error extracting adverse event data: {str(e)}")
        raise


def transform_to_standard(event_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform extracted data to standardized internal format.
    
    Args:
        event_data: Extracted adverse event data
        
    Returns:
        Transformed data in standard format
    """
    try:
        # Transform to internal standard format
        standard_format = {
            "report_identifier": event_data["report_id"],
            "report_source": "E2B_R2_MEDWATCH",
            "is_serious": event_data["serious"],
            "seriousness_reasons": event_data["seriousness_criteria"],
            "received_date": event_data["receive_date"],
            "patient_identifier": event_data["patient_id"],
            "patient_demographics": {
                "age_value": event_data["patient"].get("age"),
                "age_unit": event_data["patient"].get("age_unit"),
                "gender": "F" if event_data["patient"].get("sex") == "2" else "M",
                "weight_kg": float(event_data["patient"].get("weight", 0))
            },
            "medications": [
                {
                    "name": drug["drug_name"],
                    "product_code": drug["medicinal_product"],
                    "role": "suspect" if drug["drug_characterization"] == "1" else "concomitant",
                    "indication": drug["drug_indication"],
                    "dose": drug["dosage"],
                    "administration_route": drug["route"]
                }
                for drug in event_data["drugs"]
            ],
            "adverse_reactions": [
                {
                    "term": reaction["reaction_term"],
                    "meddra_code": reaction["meddra_code"],
                    "meddra_version": reaction["meddra_version"]
                }
                for reaction in event_data["reactions"]
            ],
            "reporter_type": event_data["primary_source"].get("reporter_title", "Unknown"),
            "reporter_qualification": event_data["primary_source"].get("qualification"),
            "submitter_organization": event_data["sender"].get("organization")
        }
        
        logger.info(f"Transformed report {standard_format['report_identifier']} to standard format")
        return standard_format
        
    except Exception as e:
        logger.error(f"Error transforming data to standard format: {str(e)}")
        raise
