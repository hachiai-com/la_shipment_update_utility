# LA Shipment Update Utility

A Python utility for processing shipment data from CSV files and updating shipments via AWS-authenticated API calls.

## Overview

This tool processes shipment information from CSV files and performs create/update operations on shipments through an API. It supports capability-based execution, allowing it to be called either via command-line arguments or JSON input via stdin.

## Features

- **CSV Processing**: Reads shipment data from CSV files with columns: id, po, delApptDate, delApptTime, delApptNo
- **AWS4 Authentication**: Securely signs API requests with AWS4 authentication
- **Configuration Management**: Loads configuration from JSON files or encrypted bin files
- **Comprehensive Logging**: Detailed logging of all operations for debugging and auditing
- **Dual Input Modes**: Supports both CLI arguments and JSON stdin input
- **Error Handling**: Robust error handling with informative error messages

## Installation

### Prerequisites

- Python 3.7 or higher
- pip (Python package installer)

### Install Dependencies

```bash
pip install -r requirements.txt
```

## Configuration

Create a `config.json` file with the following structure:

```json
{
  "region": "us-east-1",
  "service": "la-shipment-api",
  "baseUrl": "https://api.example.com/",
  "apiKey": "your-api-key",
  "accessKey": "your-aws-access-key",
  "secretKey": "your-aws-secret-key"
}
```

## CSV Input Format

The input CSV should have the following columns:

| Column | Description | Format |
|--------|-------------|--------|
| id | Row identifier | Integer |
| po | Purchase order number | String |
| delApptDate | Delivery appointment date | YYYYMMDD |
| delApptTime | Delivery appointment time | HHMMSS |
| delApptNo | Delivery appointment number | String |

Example:
```csv
id,po,delApptDate,delApptTime,delApptNo
1,4610217262,20251008,230000,45099314
2,4610966613,20251006,220000,45100203
```

## Usage

### Via Command-Line Arguments

```bash
python ShipmentUtility.py -source <input.csv> -type <operation_type> -output <output_directory> [-config <config.json>]
```

**Arguments:**
- `-source`: Path to input CSV file (required)
- `-type`: Operation type: `create` or `update` (required)
- `-output`: Output directory path (required)
- `-config`: Path to config.json file (optional)

**Example:**
```bash
python ShipmentUtility.py \
  -source shipments.csv \
  -type update \
  -output ./results \
  -config ./config.json
```

### Via JSON Input (stdin)

```bash
cat input.json | python ShipmentUtility.py
```

**input.json format:**
```json
{
  "capability": "la_shipment_update",
  "args": {
    "csv_path": "/path/to/input.csv",
    "type_operation": "create",
    "output_path": "/path/to/output",
    "config_path": "/path/to/config.json"
  }
}
```

## Output

The tool generates an `output.csv` file in the specified output directory with the following columns:

| Column | Description |
|--------|-------------|
| po | Purchase order number |
| Shipment Number | ID of the updated/created shipment |
| Notification Reason | Reason for notification (if any) |
| API Response | HTTP status code |
| Request Json | JSON payload sent to API |
| Response Json | JSON response from API |

## Capabilities

### la_shipment_update

Updates or creates shipments by processing CSV data and making API calls.

**Parameters:**
- `csv_path` (required): Absolute path to CSV file
- `type_operation` (required): Operation type (`create` or `update`)
- `output_path` (required): Output directory path
- `config_path` (optional): Path to config.json

**Response:**
```json
{
  "status": "success",
  "message": "Processed X records",
  "output_file": "/path/to/output.csv",
  "capability": "la_shipment_update"
}
```

## Logging

The utility provides detailed logging output. Configure logging in the script to adjust the verbosity level:

```python
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG for more details
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
```

## Error Handling

The tool provides informative error messages for common issues:

- Missing CSV file
- Invalid CSV format
- Missing required configuration
- API communication errors
- Invalid JSON input

## Requirements

See `requirements.txt` for full dependency list:
- requests
- requests-aws4auth
- boto3
- openpyxl

## License

Proprietary - Logistics Alliance

## Author

Ali Imran