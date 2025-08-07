"""Dune tap class."""

from datetime import datetime
from typing import List
import time

import requests
from singer_sdk import Tap
from singer_sdk import typing as th
from singer_sdk.exceptions import FatalAPIError
from singer_sdk.streams import Stream

from tap_dune.streams import DuneQueryStream


class TapDune(Tap):
    """Singer tap for Dune Analytics."""

    name = "tap-dune"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            secret=True,
            description="The API key to authenticate against the Dune API"
        ),
        th.Property(
            "base_url",
            th.StringType,
            default="https://api.dune.com/api/v1",
            description="The base URL for the Dune API"
        ),
        th.Property(
            "query_id",
            th.StringType,
            required=True,
            description="The ID of the Dune query to execute"
        ),
        th.Property(
            "query_parameters",
            th.ArrayType(
                th.ObjectType(
                    th.Property("key", th.StringType, required=True, description="Parameter key"),
                    th.Property("value", th.StringType, required=True, description="Parameter value"),
                    th.Property("replication_key", th.BooleanType, required=False, default=False, 
                              description="Whether this parameter should be used for incremental replication"),
                    th.Property("replication_key_field", th.StringType, required=False,
                              description="The field in the query results to use for tracking replication state"),
                    th.Property("type", th.StringType, required=False, default="string",
                              allowed_values=["string", "integer", "number", "date", "date-time"],
                              description="The data type of the parameter value"),
                )
            ),
            required=False,
            description="SQL Query parameters with optional replication key configuration"
        ),
        th.Property(
            "performance",
            th.StringType,
            required=False,
            default="medium",
            allowed_values=["medium", "large"],
            description="The performance engine tier: 'medium' (10 credits) or 'large' (20 credits)"
        ),
        th.Property(
            "schema",
            th.ObjectType(
                th.Property(
                    "properties",
                    th.ObjectType(
                        additional_properties=th.ObjectType(
                            th.Property("type", th.StringType, required=True),
                            th.Property("format", th.StringType, required=False),
                            th.Property("items", th.ObjectType(), required=False),
                            th.Property("properties", th.ObjectType(), required=False),
                            th.Property("required", th.ArrayType(th.StringType), required=False)
                        )
                    ),
                    required=True,
                    description="JSON Schema properties for the query result fields"
                )
            ),
            required=False,
            description="Optional: JSON Schema definition for the query result fields. If not provided, schema will be inferred from query results."
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        # Find the replication key parameter if any
        replication_key = None
        replication_key_type = None
        for param in self.config.get("query_parameters", []):
            if param.get("replication_key"):
                replication_key = param["key"]
                replication_key_type = param.get("type", "string")
                break

        # Create base schema with execution metadata fields
        execution_metadata = {
            "execution_id": {"type": "string"},
            "execution_time": {"type": "string", "format": "date-time"}
        }

        # Add replication key to schema (required by SDK) but we won't include it in output
        if replication_key:
            # Convert parameter type to schema type
            if replication_key_type in ["date", "date-time"]:
                execution_metadata[replication_key] = {"type": "string", "format": replication_key_type}
            elif replication_key_type in ["integer", "number"]:
                execution_metadata[replication_key] = {"type": replication_key_type}
            else:
                execution_metadata[replication_key] = {"type": "string"}

        # Add fields from config schema if provided, else infer from query results
        if self.config.get("schema"):
            # Start with config schema
            schema = {
                "type": "object",
                "properties": {
                    **execution_metadata,  # Put metadata first
                    **self.config["schema"]["properties"]  # Then user fields
                }
            }
        else:
            # Execute query to get sample results
            url = f"{self.config['base_url']}/query/{self.config['query_id']}/execute"
            headers = {"x-dune-api-key": self.config["api_key"]}
            params = {
                "performance": self.config.get("performance", "medium")
            }
            
            # Add query parameters if any
            if self.config.get("query_parameters"):
                params["query_parameters"] = {
                    p["key"]: p["value"] 
                    for p in self.config["query_parameters"]
                }
            
            try:
                # Log the full request details
                self.logger.info("Executing Dune query", extra={
                    "request": {
                        "method": "POST",
                        "url": url,
                        "headers": {
                            **headers,
                            "x-dune-api-key": "***"  # Mask the API key
                        },
                        "params": params
                    },
                    "query_id": self.config["query_id"]
                })
                
                response = requests.post(url, headers=headers, json=params)
                self.logger.debug("Query execution response", extra={
                    "response": {
                        "status_code": response.status_code,
                        "body": response.text
                    }
                })
                response.raise_for_status()  # Raises HTTPError for 4XX/5XX status codes
                response_data = response.json()
                
                if "execution_id" not in response_data:
                    error_msg = response_data.get("error", "No execution ID returned")
                    self.logger.error("Failed to get execution ID", extra={
                        "response": response_data,
                        "error": error_msg
                    })
                    raise FatalAPIError(f"Failed to execute query: {error_msg}")
                    
                execution_id = response_data["execution_id"]
                self.logger.info("Query execution started", extra={
                    "execution_id": execution_id
                })
            except requests.exceptions.RequestException as e:
                self.logger.error("Query execution request failed", extra={
                    "error": str(e),
                    "url": url,
                    "params": params
                })
                raise FatalAPIError(f"Failed to execute query: {str(e)}")
            
            # Wait for query completion
            while True:
                try:
                    status_url = f"{self.config['base_url']}/execution/{execution_id}/status"
                    self.logger.debug("Checking query status", extra={
                        "request": {
                            "method": "GET",
                            "url": status_url,
                            "headers": {
                                **headers,
                                "x-dune-api-key": "***"  # Mask the API key
                            }
                        },
                        "execution_id": execution_id
                    })
                    
                    status_response = requests.get(status_url, headers=headers)
                    self.logger.debug("Status check response", extra={
                        "response": {
                            "status_code": status_response.status_code,
                            "body": status_response.text
                        }
                    })
                    status_response.raise_for_status()
                    status_data = status_response.json()
                    
                    if "state" not in status_data:
                        self.logger.error("Invalid status response", extra={
                            "response": status_data
                        })
                        raise FatalAPIError("Invalid status response: missing state field")
                    
                    state = status_data["state"]
                    self.logger.debug("Query status", extra={
                        "execution_id": execution_id,
                        "state": state
                    })
                    
                    if state == "QUERY_STATE_COMPLETED":
                        self.logger.info("Query execution completed", extra={
                            "execution_id": execution_id
                        })
                        break
                    elif state in ["QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"]:
                        error_msg = status_data.get("error")
                        if error_msg:
                            self.logger.error("Query execution failed", extra={
                                "execution_id": execution_id,
                                "error": error_msg
                            })
                            raise FatalAPIError(f"Query execution failed: {error_msg}")
                        else:
                            # Check for additional error information
                            error_type = status_data.get("error_type", "Unknown error type")
                            error_details = status_data.get("error_details", "No details available")
                            self.logger.error("Query execution failed", extra={
                                "execution_id": execution_id,
                                "error_type": error_type,
                                "error_details": error_details
                            })
                            raise FatalAPIError(f"Query execution failed: {error_type} - {error_details}")
                    
                    time.sleep(2)
                except requests.exceptions.RequestException as e:
                    self.logger.error("Failed to check query status", extra={
                        "execution_id": execution_id,
                        "error": str(e)
                    })
                    raise FatalAPIError(f"Failed to check query status: {str(e)}")
            
            # Get results
            try:
                results_url = f"{self.config['base_url']}/execution/{execution_id}/results"
                self.logger.debug("Fetching query results", extra={
                    "request": {
                        "method": "GET",
                        "url": results_url,
                        "headers": {
                            **headers,
                            "x-dune-api-key": "***"  # Mask the API key
                        }
                    },
                    "execution_id": execution_id
                })
                
                results_response = requests.get(results_url, headers=headers)
                self.logger.debug("Results fetch response", extra={
                    "response": {
                        "status_code": results_response.status_code,
                        "body": results_response.text
                    }
                })
                results_response.raise_for_status()
                results_data = results_response.json()
                
                if "result" not in results_data or "rows" not in results_data["result"]:
                    self.logger.error("Invalid results response", extra={
                        "execution_id": execution_id,
                        "response": results_data
                    })
                    raise FatalAPIError("Invalid results response: missing result data")
                
                self.logger.info("Query results fetched", extra={
                    "execution_id": execution_id,
                    "row_count": len(results_data["result"]["rows"])
                })
                
                # Create schema with execution metadata
                schema = {
                    "type": "object",
                    "properties": {
                        **execution_metadata
                    }
                }
            except requests.exceptions.RequestException as e:
                raise FatalAPIError(f"Failed to fetch query results: {str(e)}")

            # Infer schema from results
            if results_data["result"]["rows"]:
                sample_row = results_data["result"]["rows"][0]
                for key, value in sample_row.items():
                    if value is None:
                        # For null values, check other rows for a non-null value
                        for row in results_data["result"]["rows"][1:]:
                            if row.get(key) is not None:
                                value = row[key]
                                break
                        if value is None:
                            # If still null, default to string type
                            schema["properties"][key] = {"type": "string"}
                            continue
                    
                    if isinstance(value, bool):
                        schema["properties"][key] = {"type": "boolean"}
                    elif isinstance(value, int):
                        schema["properties"][key] = {"type": "integer"}
                    elif isinstance(value, float):
                        schema["properties"][key] = {"type": "number"}
                    elif isinstance(value, str):
                        # Try to detect date/datetime formats
                        try:
                            datetime.strptime(value, "%Y-%m-%d")
                            schema["properties"][key] = {"type": "string", "format": "date"}
                        except ValueError:
                            try:
                                datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
                                schema["properties"][key] = {"type": "string", "format": "date-time"}
                            except ValueError:
                                schema["properties"][key] = {"type": "string"}
                    elif isinstance(value, (list, tuple)):
                        schema["properties"][key] = {
                            "type": "array",
                            "items": {"type": "string"}  # Simplified - could be enhanced
                        }
                    elif isinstance(value, dict):
                        schema["properties"][key] = {
                            "type": "object",
                            "properties": {}  # Simplified - could be enhanced
                        }
                    else:
                        schema["properties"][key] = {"type": "string"}

        # Create stream with schema and replication key
        stream = DuneQueryStream(
            tap=self,
            name="dune_query",
            query_id=self.config["query_id"],
            schema=schema,
            replication_key=replication_key,
            replication_key_type=replication_key_type
        )

        return [stream]