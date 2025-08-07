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
        replication_key_format = None
        for param in self.config.get("query_parameters", []):
            if param.get("replication_key"):
                replication_key = param["key"]
                # Try to determine the format from the schema if available
                if self.config.get("schema") and replication_key in self.config["schema"].get("properties", {}):
                    replication_key_format = self.config["schema"]["properties"][replication_key].get("format")
                break

        # Create base schema with execution metadata fields
        execution_metadata = {
            "execution_id": {"type": "string"},
            "execution_time": {"type": "string", "format": "date-time"}
        }

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
            
            response = requests.post(url, headers=headers, json=params)
            if response.status_code != 200:
                raise FatalAPIError(f"Failed to execute query: {response.text}")
                
            execution_id = response.json()["execution_id"]
            
            # Wait for query completion
            while True:
                status_response = requests.get(
                    f"{self.config['base_url']}/execution/{execution_id}/status",
                    headers=headers
                )
                status_data = status_response.json()
                
                if status_data["state"] == "QUERY_STATE_COMPLETED":
                    break
                elif status_data["state"] in ["QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"]:
                    raise FatalAPIError(f"Query execution failed: {status_data.get('error')}")
                
                time.sleep(2)
            
            # Get results
            results_response = requests.get(
                f"{self.config['base_url']}/execution/{execution_id}/results",
                headers=headers
            )
            results_data = results_response.json()
            
            # Create schema with execution metadata
            schema = {
                "type": "object",
                "properties": {
                    **execution_metadata
                }
            }

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

        # Add replication key field if not already in schema
        if replication_key and replication_key not in schema["properties"]:
            schema["properties"][replication_key] = {
                "type": "string",
                "format": replication_key_format or "date-time"  # Default to date-time if format not specified
            }
        
        # Create stream with schema and replication key
        stream = DuneQueryStream(
            tap=self,
            name="dune_query",
            query_id=self.config["query_id"],
            schema=schema
        )

        # Set replication key if found
        if replication_key:
            stream.replication_key = replication_key

        return [stream]