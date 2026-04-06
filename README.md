# E2B R2 MedWatch Form Parser Pipeline

An Apache Airflow 3.1.0 pipeline for processing FDA MedWatch E2B(R2) XML forms for adverse event reporting.

## Overview

This pipeline automates the extraction, validation, and processing of E2B(R2) Individual Case Safety Reports (ICSRs) submitted to the FDA MedWatch system.

### Features

- ✅ **E2B R2 XML Parsing**: Processes ICH E2B(R2) format adverse event reports
- ✅ **Schema Validation**: Validates forms against E2B R2 standards
- ✅ **Data Extraction**: Extracts patient, drug, and adverse event information
- ✅ **Standardization**: Transforms data to internal standard format
- ✅ **Error Handling**: Comprehensive error handling and retry logic
- ✅ **Logging**: Detailed logging for audit and troubleshooting

## Pipeline Architecture

### Workflow Steps

1. **Initialize Pipeline** - Setup and prepare environment
2. **Scan XML Files** - Discover new E2B R2 XML files in input directory
3. **Parse E2B Forms** - Extract structured data from XML
4. **Validate Forms** - Check against E2B R2 schema and business rules
5. **Extract Adverse Events** - Extract patient, drug, and reaction data
6. **Transform Data** - Convert to standardized internal format
7. **Load to Warehouse** - Store processed data
8. **Generate Report** - Create summary of pipeline execution

### Data Flow

```
Input Directory → XML Parser → Validator → Extractor → Transformer → Data Warehouse
```

## Configuration

### Schedule
- **Default**: Daily at midnight UTC (`@daily`)
- **Start Date**: 2025-01-01
- **Catchup**: Disabled

### Retry Policy
- **Retries**: 3 attempts
- **Retry Delay**: 5 minutes
- **Execution Timeout**: 30 minutes per task

## File Structure

```
.
├── src/
│   ├── main.py                    # Main DAG definition
│   └── tasks/
│       ├── form_parser.py         # XML parsing and data extraction
│       └── data_validator.py      # Form validation logic
├── config.yaml                    # Pipeline configuration
├── requirements.txt               # Python dependencies
└── README.md                      # This file
```

## Dependencies

### Core Libraries
- `apache-airflow-sdk>=3.1.0` - Airflow SDK
- `lxml>=4.9.0` - XML processing
- `xmltodict>=0.13.0` - XML to dictionary conversion
- `pandas>=2.0.0` - Data manipulation
- `pendulum>=3.0.0` - Date/time handling

### Validation
- `jsonschema>=4.17.0` - JSON schema validation
- `pydantic>=2.0.0` - Data validation
- `xmlschema>=2.5.0` - XML schema validation

## E2B(R2) Format

The pipeline processes Individual Case Safety Reports (ICSRs) in ICH E2B(R2) format, which includes:

### Key Data Elements

- **Message Header**: Sender, receiver, transmission metadata
- **Safety Report**: Report identification, type, dates
- **Patient Information**: Demographics, medical history
- **Drug Information**: Suspect/concomitant medications, dosage, route
- **Adverse Reactions**: MedDRA coded adverse events
- **Narrative**: Case description and outcome

### Report Types
1. Spontaneous report
2. Report from study
3. Other sources
4. Not available

### Seriousness Criteria
- Death
- Life-threatening
- Hospitalization
- Disability
- Congenital anomaly
- Other medically important condition

## Input Format

Place E2B R2 XML files in the input directory:
```
/data/input/medwatch/*.xml
```

### Sample XML Structure
```xml
<?xml version="1.0" encoding="UTF-8"?>
<ichicsr>
  <ichicsrmessageheader>
    <messagetype>E2B(R2)</messagetype>
    <messageformatversion>2.1</messageformatversion>
  </ichicsrmessageheader>
  <safetyreport>
    <safetyreportid>US-COMPANY-2025-001234</safetyreportid>
    <patient>
      <!-- Patient data -->
    </patient>
    <drug>
      <!-- Drug data -->
    </drug>
    <reaction>
      <!-- Adverse reaction data -->
    </reaction>
  </safetyreport>
</ichicsr>
```

## Output Format

Transformed data in standardized internal format:

```json
{
  "report_identifier": "US-COMPANY-2025-001234",
  "report_source": "E2B_R2_MEDWATCH",
  "is_serious": true,
  "patient_demographics": {
    "age_value": "45",
    "gender": "F",
    "weight_kg": 70.0
  },
  "medications": [
    {
      "name": "Sample Drug A",
      "role": "suspect",
      "indication": "Hypertension"
    }
  ],
  "adverse_reactions": [
    {
      "term": "Nausea",
      "meddra_code": "10028813"
    }
  ]
}
```

## Usage

### Running the Pipeline

```bash
# Trigger manually
airflow dags trigger e2b_r2_medwatch_parser

# View logs
airflow tasks logs e2b_r2_medwatch_parser initialize_pipeline <execution_date>
```

### Monitoring

Check pipeline status in Airflow UI:
- DAG: `e2b_r2_medwatch_parser`
- Tags: `medwatch`, `e2b`, `fda`, `adverse-events`

## Validation Rules

### Required Fields
- Safety report ID
- Report type
- Receive date
- At least one adverse reaction

### Data Quality Checks
- Report ID format validation
- Date format verification (YYYYMMDD)
- MedDRA code validation
- Age/weight range checks
- Drug characterization codes

## Error Handling

- **Parse Errors**: Logged and tracked; processing continues with valid forms
- **Validation Failures**: Invalid forms separated for review
- **Load Errors**: Automatic retry with exponential backoff

## Compliance

This pipeline is designed to process adverse event data in compliance with:
- ICH E2B(R2) standards
- FDA MedWatch requirements
- 21 CFR Part 11 (when configured with appropriate infrastructure)

## Development

### Testing

```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
pytest tests/

# Run with coverage
pytest --cov=src tests/
```

### Extending the Pipeline

To add custom validation rules, modify:
```python
src/tasks/data_validator.py
```

To customize transformation logic, modify:
```python
src/tasks/form_parser.py::transform_to_standard()
```

## Support

For issues or questions:
1. Check Airflow logs for detailed error messages
2. Review validation errors in pipeline output
3. Verify XML format matches E2B(R2) specification

## License

Internal use only. Ensure compliance with data privacy regulations when processing patient data.

## Version History

- **1.0.0** (2025-01-01): Initial release
  - E2B R2 XML parsing
  - Schema validation
  - Data standardization
  - Warehouse loading
