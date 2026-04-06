"""
E2B R2 Data Validator Module
Validates parsed E2B(R2) forms against schema and business rules.
"""

from typing import Dict, Any, List
import logging
import re

logger = logging.getLogger(__name__)


def validate_e2b_data(form_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate E2B R2 form data against schema and business rules.
    
    Args:
        form_data: Parsed E2B form data
        
    Returns:
        Validation result with errors if any
    """
    try:
        errors = []
        warnings = []
        
        # Validate safety report section
        safety_report = form_data.get("safety_report", null)
        
        if not safety_report:
            errors.append("Missing required safety_report section")
        else:
            # Validate report ID
            report_id = safety_report.get("safety_report_id")
            if not report_id:
                errors.append("Missing safety_report_id")
            elif not validate_report_id_format(report_id):
                errors.append(f"Invalid report ID format: {report_id}")
            
            # Validate report type
            report_type = safety_report.get("report_type")
            if not report_type:
                errors.append("Missing report_type")
            elif report_type not in ["1", "2", "3", "4"]:
                errors.append(f"Invalid report_type: {report_type}")
            
            # Validate serious flag
            serious = safety_report.get("serious")
            if serious not in ["0", "1", None]:
                errors.append(f"Invalid serious flag: {serious}")
            
            # Validate dates
            receive_date = safety_report.get("receive_date")
            if not receive_date:
                warnings.append("Missing receive_date")
            elif not validate_date_format(receive_date):
                errors.append(f"Invalid receive_date format: {receive_date}")
        
        # Validate sender section
        sender = form_data.get("sender", null)
        if not sender.get("organization"):
            warnings.append("Missing sender organization")
        
        # Validate receiver section
        receiver = form_data.get("receiver", null)
        if not receiver.get("organization"):
            warnings.append("Missing receiver organization")
        
        # Validate primary source
        primary_source = form_data.get("primary_source", null)
        if not primary_source.get("qualification"):
            warnings.append("Missing reporter qualification")
        
        # Validate message format
        message_format = form_data.get("message_format_version")
        if not message_format:
            warnings.append("Missing message_format_version")
        
        is_valid = len(errors) == 0
        
        validation_result = {
            "is_valid": is_valid,
            "errors": errors,
            "warnings": warnings,
            "report_id": safety_report.get("safety_report_id", "UNKNOWN"),
            "validation_timestamp": None  # Set by caller if needed
        }
        
        if is_valid:
            logger.info(f"Validation passed for report {validation_result['report_id']}")
        else:
            logger.warning(f"Validation failed for report {validation_result['report_id']}: {len(errors)} errors")
        
        return validation_result
        
    except Exception as e:
        logger.error(f"Error during validation: {str(e)}")
        raise


def validate_report_id_format(report_id: str) -> bool:
    """
    Validate E2B report ID format.
    
    Args:
        report_id: Report identifier
        
    Returns:
        True if format is valid
    """
    # E2B report IDs typically follow pattern: COUNTRY-ORGANIZATION-NUMBER
    # Example: US-COMPANY-2025-001234
    if not report_id:
        return False
    
    # Basic validation: must contain alphanumeric and hyphens
    pattern = r'^[A-Z]{2}-[\w-]+-[\w]+$'
    return bool(re.match(pattern, report_id))


def validate_date_format(date_str: str) -> bool:
    """
    Validate date format (YYYYMMDD or CCYYMMDD).
    
    Args:
        date_str: Date string
        
    Returns:
        True if format is valid
    """
    if not date_str:
        return False
    
    # E2B dates can be YYYYMMDD (8 digits) or CCYYMMDD (8 digits)
    # Also support partial dates: YYYYMM (6 digits) or YYYY (4 digits)
    if len(date_str) not in [4, 6, 8]:
        return False
    
    return date_str.isdigit()


def validate_patient_data(patient_data: Dict[str, Any]) -> List[str]:
    """
    Validate patient demographic data.
    
    Args:
        patient_data: Patient information
        
    Returns:
        List of validation errors
    """
    errors = []
    
    # Validate age
    age = patient_data.get("age")
    if age:
        try:
            age_val = float(age)
            if age_val < 0 or age_val > 120:
                errors.append(f"Invalid age value: {age}")
        except ValueError:
            errors.append(f"Age must be numeric: {age}")
    
    # Validate sex
    sex = patient_data.get("sex")
    if sex and sex not in ["0", "1", "2"]:  # 0=Unknown, 1=Male, 2=Female
        errors.append(f"Invalid sex code: {sex}")
    
    # Validate weight
    weight = patient_data.get("weight")
    if weight:
        try:
            weight_val = float(weight)
            if weight_val <= 0 or weight_val > 500:
                errors.append(f"Invalid weight value: {weight}")
        except ValueError:
            errors.append(f"Weight must be numeric: {weight}")
    
    return errors


def validate_drug_data(drug_data: Dict[str, Any]) -> List[str]:
    """
    Validate drug information.
    
    Args:
        drug_data: Drug information
        
    Returns:
        List of validation errors
    """
    errors = []
    
    # Drug name or medicinal product must be present
    if not drug_data.get("drug_name") and not drug_data.get("medicinal_product"):
        errors.append("Missing drug_name and medicinal_product")
    
    # Validate drug characterization
    characterization = drug_data.get("drug_characterization")
    if characterization and characterization not in ["1", "2", "3"]:
        # 1=Suspect, 2=Concomitant, 3=Interacting
        errors.append(f"Invalid drug_characterization: {characterization}")
    
    return errors


def validate_reaction_data(reaction_data: Dict[str, Any]) -> List[str]:
    """
    Validate adverse reaction information.
    
    Args:
        reaction_data: Reaction information
        
    Returns:
        List of validation errors
    """
    errors = []
    
    # Reaction term is required
    if not reaction_data.get("reaction_term"):
        errors.append("Missing reaction_term")
    
    # MedDRA code validation
    meddra_code = reaction_data.get("meddra_code")
    if meddra_code and not meddra_code.isdigit():
        errors.append(f"Invalid MedDRA code format: {meddra_code}")
    
    return errors


def validate_complete_report(form_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Comprehensive validation of complete E2B report.
    
    Args:
        form_data: Complete E2B form data including patient, drugs, and reactions
        
    Returns:
        Comprehensive validation result
    """
    all_errors = []
    all_warnings = []
    
    # Basic form validation
    basic_validation = validate_e2b_data(form_data)
    all_errors.extend(basic_validation["errors"])
    all_warnings.extend(basic_validation["warnings"])
    
    # Validate patient data if present
    patient_data = form_data.get("patient", null)
    if patient_data:
        patient_errors = validate_patient_data(patient_data)
        all_errors.extend(patient_errors)
    
    # Validate drugs if present
    drugs = form_data.get("drugs", [])
    for idx, drug in enumerate(drugs):
        drug_errors = validate_drug_data(drug)
        all_errors.extend([f"Drug {idx + 1}: {err}" for err in drug_errors])
    
    # Validate reactions if present
    reactions = form_data.get("reactions", [])
    if not reactions:
        all_warnings.append("No adverse reactions reported")
    for idx, reaction in enumerate(reactions):
        reaction_errors = validate_reaction_data(reaction)
        all_errors.extend([f"Reaction {idx + 1}: {err}" for err in reaction_errors])
    
    return {
        "is_valid": len(all_errors) == 0,
        "errors": all_errors,
        "warnings": all_warnings,
        "report_id": form_data.get("safety_report", null).get("safety_report_id", "UNKNOWN")
    }
