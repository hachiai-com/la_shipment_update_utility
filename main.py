import sys
import json
import csv
import logging
import requests
from urllib.parse import urlparse, parse_qs, unquote
from datetime import datetime
from typing import Dict, List, Optional, Any
import os
from pathlib import Path
import argparse
from requests_aws4auth import AWS4Auth
import boto3

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

CAPABILITY_NAME = "la_shipment_update"

# Global configuration variables
REGION = ""
SERVICE = ""
BASE_URL = ""
API_KEY = ""
ACCESS_KEY = ""
SECRET_KEY = ""


class ShipmentUtility:
    """Utility class for handling shipment creation and updates"""
    
    def __init__(self):
        self.region = REGION
        self.service = SERVICE
        self.base_url = BASE_URL
        self.api_key = API_KEY
        self.access_key = ACCESS_KEY
        self.secret_key = SECRET_KEY
    
    @staticmethod
    def parse_args(args: List[str]) -> Dict[str, str]:
        """Parse command-line arguments"""
        parser = argparse.ArgumentParser(description='Shipment Utility')
        parser.add_argument('-source', required=True, help='CSV file for processing')
        parser.add_argument('-type', required=True, help='Type of operation: create or update')
        parser.add_argument('-output', required=True, help='Output directory path')
        parser.add_argument('-config', required=False, help='Path to config.json')
        
        parsed = parser.parse_args(args)
        return {
            'source': parsed.source,
            'type': parsed.type,
            'output': parsed.output,
            'config': parsed.config
        }
    
    def load_config(self, config_path: Optional[str] = None) -> None:
        """Load configuration from config.json or encrypted bin file"""
        global REGION, SERVICE, BASE_URL, API_KEY, ACCESS_KEY, SECRET_KEY
        
        # Try to load from config_path first if provided
        if config_path and os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    cfg = json.load(f)
                    # logger.info(f"Config file contents: {json.dumps(cfg, indent=2)}")
                    
                    REGION = cfg.get('region', REGION)
                    SERVICE = cfg.get('service', SERVICE)
                    BASE_URL = cfg.get('baseUrl', BASE_URL)
                    API_KEY = cfg.get('apiKey', API_KEY)
                    ACCESS_KEY = cfg.get('accessKey', ACCESS_KEY)
                    SECRET_KEY = cfg.get('secretKey', SECRET_KEY)
                    
                    # Update instance variables
                    self.region = REGION
                    self.service = SERVICE
                    self.base_url = BASE_URL
                    self.api_key = API_KEY
                    self.access_key = ACCESS_KEY
                    self.secret_key = SECRET_KEY
                    
                    logger.info(f"Configuration loaded from {config_path}")
                    return
            except Exception as e:
                logger.error(f"Error loading config from {config_path}: {str(e)}")
        
        # Fall back to encrypted bin file
        if os.path.exists("la-aws-data.bin"):
            logger.info("bin file found")
            try:
                with open("la-aws-data.bin", 'r') as f:
                    encrypted_json = f.readline().strip()
                    # Note: You'll need to implement decryption based on your JavaEncryptionService
                    # For now, assuming the file contains plain JSON or implement decryption
                    dec_text = self._decrypt_text(encrypted_json)
                    json_data = json.loads(dec_text)
                    
                    REGION = json_data.get('region', REGION)
                    SERVICE = json_data.get('service', SERVICE)
                    BASE_URL = json_data.get('baseUrl', BASE_URL)
                    API_KEY = json_data.get('apiKey', API_KEY)
                    ACCESS_KEY = json_data.get('accessKey', ACCESS_KEY)
                    SECRET_KEY = json_data.get('secretKey', SECRET_KEY)
                    
                    # Update instance variables
                    self.region = REGION
                    self.service = SERVICE
                    self.base_url = BASE_URL
                    self.api_key = API_KEY
                    self.access_key = ACCESS_KEY
                    self.secret_key = SECRET_KEY
                    
                    logger.info("Configuration loaded from la-aws-data.bin")
            except Exception as e:
                logger.error(f"Issue reading data from bin file: {str(e)}")
        else:
            logger.error("bin file not found and no config_path provided")
    
    @staticmethod
    def _decrypt_text(encrypted_text: str) -> str:
        """
        Decrypt text from Java encryption service.
        This is a placeholder - implement according to your JavaEncryptionService.
        """
        # TODO: Implement decryption logic matching JavaEncryptionService
        return encrypted_text
    
    def get_params_from_url(self, url: str) -> Dict[str, str]:
        """Extract query parameters from URL"""
        params = {}
        try:
            parsed_url = urlparse(url)
            query_string = parsed_url.query
            
            if not query_string:
                logger.warning(f"URL has no query parameters: {url}")
                return params
            
            for param in query_string.split('&'):
                if '=' in param:
                    key, value = param.split('=', 1)
                    try:
                        key = unquote(key)
                        value = unquote(value) if value else ""
                        params[key] = value
                    except Exception as e:
                        logger.error(f"Error decoding URL parameter: {param}, {str(e)}")
                else:
                    params[unquote(param)] = ""
        
        except Exception as e:
            logger.error(f"Error parsing URL: {url}, {str(e)}")
        
        return params
    
    def get_shipment_id(self, json_str: str) -> Optional[str]:
        """Extract shipment_id from JSON response"""
        if not json_str or not json_str.strip():
            logger.warning("Input JSON string is null or empty.")
            return None
        
        try:
            data = json.loads(json_str)
            shipments = data.get('shipments', [])
            
            if not shipments or not isinstance(shipments, list):
                logger.warning("'shipments' array is missing or empty.")
                return None
            
            first_shipment = shipments[0]
            shipment_id = first_shipment.get('shipment_id')
            
            if not shipment_id:
                logger.warning("'shipment_id' field is missing.")
                return None
            
            return shipment_id
        except Exception as e:
            logger.error(f"Error parsing JSON for shipment_id: {str(e)}")
            return None
    
    def generate_update_payload(self, params: Dict[str, str]) -> Dict[str, Any]:
        """Generate update payload from parameters"""
        payload = {}
        
        try:
            del_appt_date = params.get('delApptDate')
            del_appt_time = params.get('delApptTime')
            del_appt_no = params.get('delApptNo')
            
            if not all([del_appt_date, del_appt_time, del_appt_no]):
                logger.warning(f"Missing parameters for update payload: {params}")
                return payload
            
            # Parse and format date
            date_obj = datetime.strptime(del_appt_date, '%Y%m%d')
            formatted_date = date_obj.strftime('%Y-%m-%d')
            
            # Parse and format time
            time_obj = datetime.strptime(del_appt_time, '%H%M%S')
            formatted_time = time_obj.strftime('%H:%M:%S')
            
            payload['dates'] = {
                'delivery_appointment_date': formatted_date,
                'delivery_time_from': formatted_time,
                'delivery_appointment': del_appt_no
            }
        
        except Exception as e:
            logger.error(f"Error generating update payload: {str(e)}")
        
        return payload
    
    def search_shipment(self, payload: str) -> Dict[str, str]:
        """Search for shipment"""
        return self.call_api('search', payload, 'POST')
    
    def update_shipment(self, payload: str, shipment_id: str) -> Dict[str, str]:
        """Update shipment"""
        return self.call_api(shipment_id, payload, 'PATCH')
    
    def call_api(self, resource_path: str, payload: str, method: str) -> Dict[str, str]:
        """Make API call with AWS4 signature"""
        response_map = {}
        url = self.base_url + resource_path
        
        try:
            headers = {
                'x-api-key': self.api_key,
                'Content-Type': 'application/json'
            }
            
            # Create AWS4 auth
            auth = AWS4Auth(self.access_key, self.secret_key, self.region, self.service)
            
            if method == 'POST':
                response = requests.post(url, data=payload, headers=headers, auth=auth)
            elif method == 'PATCH':
                response = requests.patch(url, data=payload, headers=headers, auth=auth)
            else:
                response = requests.request(method, url, data=payload, headers=headers, auth=auth)
            
            response_map['code'] = str(response.status_code)
            response_map['body'] = response.text
            
            logger.info(f"API call [{method} {url}] - Status: {response.status_code}")
            logger.info(f"Response body: {response.text}")
        
        except Exception as e:
            logger.error(f"Error calling API [{method} {url}]: {str(e)}")
            response_map['body'] = str(e)
        
        return response_map
    
    def write_records_to_csv(self, file_path: str, records: List[List[str]]) -> None:
        """Write records to CSV file"""
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            with open(file_path, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                
                # Write header
                header = ['po', 'Shipment Number', 'Notification Reason', 'API Response', 'Request Json', 'Response Json']
                writer.writerow(header)
                
                # Write records
                for record in records:
                    if record and len(record) == 6:
                        writer.writerow(record)
                    else:
                        logger.warning(f"Skipping invalid record: {record}")
            
            logger.info(f"Records written successfully to {file_path}")
        
        except Exception as e:
            logger.error(f"Error writing CSV: {file_path}, {str(e)}")
    
    def process_shipment_creation(self, csv_path: str, type_operation: str, 
                                  output_path: str, config_path: Optional[str] = None) -> Dict[str, Any]:
        """Main processing function for shipment creation/update"""
        try:
            # Load configuration
            self.load_config(config_path)
            
            output_records = []
            
            # Read CSV file
            if not os.path.exists(csv_path):
                return {
                    'status': 'error',
                    'error': f'CSV file not found: {csv_path}',
                    'capability': CAPABILITY_NAME
                }
            
            with open(csv_path, 'r') as csvfile:
                reader = csv.reader(csvfile)
                records = list(reader)
            
            logger.info(f"CSV file contents: {json.dumps(records, indent=2)}")
            
            if len(records) <= 1:
                logger.warning("No data rows found in CSV.")
                return {
                    'status': 'success',
                    'message': 'No data rows found in CSV',
                    'capability': CAPABILITY_NAME
                }
            
            # Process each row
            for i in range(1, len(records)):
                try:
                    row = records[i]
                    
                    if len(row) < 5:
                        logger.warning(f"Skipping invalid row at index {i}: expected at least 5 columns")
                        continue
                    
                    # Extract values from CSV columns directly
                    # Columns: id, po, delApptDate, delApptTime, delApptNo
                    po = row[1].strip()
                    del_appt_date = row[2].strip()
                    del_appt_time = row[3].strip()
                    del_appt_no = row[4].strip()
                    
                    if not po:
                        logger.warning(f"Missing purchase order in row {i}")
                        continue
                    
                    # Create params dict from CSV columns
                    params = {
                        'po': po,
                        'delApptDate': del_appt_date,
                        'delApptTime': del_appt_time,
                        'delApptNo': del_appt_no
                    }
                    
                    logger.info(f"Processing row {i} with params: {json.dumps(params)}")
                    
                    search_payload = {'purchase_order': po}
                    search_payload_str = json.dumps(search_payload)
                    
                    search_response = self.search_shipment(search_payload_str)
                    shipment_id = self.get_shipment_id(search_response.get('body', ''))
                    
                    if shipment_id:
                        update_payload = self.generate_update_payload(params)
                        update_payload_str = json.dumps(update_payload)
                        update_response = self.update_shipment(update_payload_str, shipment_id)
                        
                        output_records.append([
                            po,
                            shipment_id,
                            '',
                            '',
                            update_payload_str,
                            update_response.get('body', '')
                        ])
                    else:
                        output_records.append([
                            po,
                            '',
                            '',
                            '',
                            search_payload_str,
                            search_response.get('body', '')
                        ])
                        logger.warning(f"Shipment not found for PO: {po}")
                
                except Exception as e:
                    logger.error(f"Error processing row {i}: {str(e)}")
            
            # Write output CSV
            self.write_records_to_csv(os.path.join(output_path, 'output.csv'), output_records)
            
            return {
                'status': 'success',
                'message': f'Processed {len(output_records)} records',
                'output_file': os.path.join(output_path, 'output.csv'),
                'capability': CAPABILITY_NAME
            }
        
        except Exception as e:
            logger.error(f"Error in process_shipment_creation: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'capability': CAPABILITY_NAME
            }


def process_shipment_creation(csv_path: str, type_operation: str, 
                             output_path: str, config_path: Optional[str] = None) -> Dict[str, Any]:
    """Wrapper function for capability-based execution"""
    utility = ShipmentUtility()
    return utility.process_shipment_creation(csv_path, type_operation, output_path, config_path)


def main():
    """Main entry point - capability-based execution"""
    try:
        # Check if there's input from stdin
        if not sys.stdin.isatty():
            # Read from stdin (piped input)
            input_data = json.load(sys.stdin)
        elif len(sys.argv) > 1:
            # Fallback to command-line arguments for backward compatibility
            params = ShipmentUtility.parse_args(sys.argv[1:])
            
            if not params.get('source') or not params.get('type') or not params.get('output'):
                logger.error("Usage: python ShipmentUtility.py -source <input.csv> -type <type> -output <output_directory_path> [-config <config_path>]")
                print(json.dumps({
                    "status": "error",
                    "error": "Missing required arguments: -source, -type, -output",
                    "capability": CAPABILITY_NAME
                }, indent=2))
                sys.exit(1)
            
            # Convert command-line args to capability format
            input_data = {
                "capability": CAPABILITY_NAME,
                "args": {
                    "csv_path": params.get('source'),
                    "type_operation": params.get('type'),
                    "output_path": params.get('output'),
                    "config_path": params.get('config')
                }
            }
        else:
            print(json.dumps({
                "status": "error",
                "error": "No input provided. Either pipe JSON via stdin or use command-line arguments.",
                "usage_json": {
                    "capability": CAPABILITY_NAME,
                    "args": {
                        "csv_path": "/path/to/input.csv",
                        "type_operation": "create or update",
                        "output_path": "/path/to/output",
                        "config_path": "/path/to/config.json (optional)"
                    }
                },
                "usage_cli": "python ShipmentUtility.py -source <input.csv> -type <type> -output <output_directory_path> [-config <config_path>]",
                "capability": "unknown"
            }, indent=2))
            sys.exit(1)

        capability = input_data.get("capability")
        args = input_data.get("args", {})

        if capability == CAPABILITY_NAME:
            response = process_shipment_creation(
                csv_path=args.get("csv_path"),
                type_operation=args.get("type_operation", "create"),
                output_path=args.get("output_path"),
                config_path=args.get("config_path")
            )
            print(json.dumps(response, indent=2))
        else:
            print(json.dumps({
                "status": "error",
                "error": f"Unknown capability: {capability}",
                "capability": capability
            }, indent=2))
            sys.exit(1)

    except json.JSONDecodeError as e:
        print(json.dumps({
            "status": "error",
            "error": f"Invalid JSON input: {str(e)}",
            "hint": "Provide JSON via stdin or use command-line arguments",
            "capability": "unknown"
        }, indent=2))
        sys.exit(1)
    except Exception as e:
        capability = locals().get('capability', 'unknown')
        print(json.dumps({
            "status": "error",
            "error": f"Error: {str(e)}",
            "capability": capability
        }, indent=2))
        sys.exit(1)


if __name__ == "__main__":
    main()
