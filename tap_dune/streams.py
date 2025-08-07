"""Stream type classes for tap-dune."""

from typing import Any, List, Optional, Iterable
import time

import requests
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import FatalAPIError


class DuneQueryStream(RESTStream):
    """Stream for executing and retrieving Dune query results with incremental replication support."""
    
    # We'll set these dynamically based on the query configuration
    replication_key = None
    is_sorted = True  # Assuming date-based parameters are sorted
    primary_keys = ["execution_id"]
    
    def infer_schema_from_results(self, results: List[dict]) -> dict:
        """Infer JSON Schema from query results.
        
        Args:
            results: List of result rows from Dune query
            
        Returns:
            JSON Schema definition for the results
        """
        properties = {}
        
        if not results:
            return {"type": "object", "properties": properties}
            
        sample_row = results[0]
        for key, value in sample_row.items():
            if value is None:
                # For null values, check other rows for a non-null value
                for row in results[1:]:
                    if row.get(key) is not None:
                        value = row[key]
                        break
                if value is None:
                    # If still null, default to string type
                    properties[key] = {"type": "string"}
                    continue
            
            if isinstance(value, bool):
                properties[key] = {"type": "boolean"}
            elif isinstance(value, int):
                properties[key] = {"type": "integer"}
            elif isinstance(value, float):
                properties[key] = {"type": "number"}
            elif isinstance(value, str):
                # Try to detect date/datetime formats
                try:
                    from datetime import datetime
                    datetime.strptime(value, "%Y-%m-%d")
                    properties[key] = {"type": "string", "format": "date"}
                except ValueError:
                    try:
                        datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f %Z")
                        properties[key] = {"type": "string", "format": "date-time"}
                    except ValueError:
                        properties[key] = {"type": "string"}
            elif isinstance(value, (list, tuple)):
                properties[key] = {
                    "type": "array",
                    "items": {"type": "string"}  # Simplified - could be enhanced
                }
            elif isinstance(value, dict):
                properties[key] = {
                    "type": "object",
                    "properties": {}  # Simplified - could be enhanced
                }
            else:
                properties[key] = {"type": "string"}
                
        return {"type": "object", "properties": properties}

    def __init__(self, tap: Any, name: str, query_id: str, schema: dict = None, **kwargs):
        """Initialize the stream.
        
        Args:
            tap: The parent tap object
            name: The stream name
            query_id: The Dune query ID
            schema: The stream schema (from query results)
        """
        self.query_id = query_id
        
        # Find replication key from parameters if any is configured
        for param in tap.config.get("query_parameters", []):
            if param.get("replication_key"):
                self.replication_key = param["key"]
                break
        
        # If schema not provided, fetch a sample and infer it
        if not schema and tap.config.get("schema") is None:
            # Execute query to get sample results
            url = f"{tap.config['base_url']}/query/{query_id}/execute"
            headers = {"x-dune-api-key": tap.config["api_key"]}
            params = {
                "performance": tap.config.get("performance", "medium")
            }
            
            # Add query parameters if any
            if tap.config.get("query_parameters"):
                params["query_parameters"] = {
                    p["key"]: p["value"] 
                    for p in tap.config["query_parameters"]
                }
            
            response = requests.post(url, headers=headers, json=params)
            if response.status_code != 200:
                raise FatalAPIError(f"Failed to execute query: {response.text}")
                
            execution_id = response.json()["execution_id"]
            
            # Wait for query completion
            while True:
                status_response = requests.get(
                    f"{tap.config['base_url']}/execution/{execution_id}/status",
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
                f"{tap.config['base_url']}/execution/{execution_id}/results",
                headers=headers
            )
            results_data = results_response.json()
            
            # Infer schema from results
            schema = self.infer_schema_from_results(results_data["result"]["rows"])
        elif tap.config.get("schema"):
            schema = tap.config["schema"]
            
        # Add execution metadata to schema
        execution_metadata = {
            "execution_id": {"type": "string"},
            "execution_time": {"type": "string", "format": "date-time"}
        }
        
        # Create final schema with execution metadata
        final_schema = {
            "type": "object",
            "properties": {
                **execution_metadata,
                **(schema.get("properties", {}) if schema else {})
            }
        }
        
        self._schema = final_schema
        super().__init__(tap, name=name, schema=final_schema, **kwargs)
    
    @property
    def schema(self) -> dict:
        """Return stream schema.
        
        Returns:
            Stream schema.
        """
        return self._schema
    
    @property
    def url_base(self) -> str:
        """Return the API URL root."""
        return self.config["base_url"]

    @property
    def path(self) -> str:
        """Return the API endpoint path for query execution."""
        return f"/query/{self.query_id}/execute"

    def get_url(self, context: Optional[dict] = None, next_page_token: Optional[Any] = None) -> str:
        """Get the URL for the request."""
        return f"{self.url_base}{self.path}"

    @property
    def http_method(self) -> str:
        """Return the HTTP method to use for requests."""
        return "POST"
    
    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers["x-dune-api-key"] = self.config["api_key"]
        headers["Content-Type"] = "application/json"
        return headers
    
    def prepare_request(self, context: Optional[dict], next_page_token: Optional[Any]) -> requests.PreparedRequest:
        """Prepare a request object for this REST stream."""
        http_method = self.http_method
        url: str = self.get_url(context, next_page_token)
        headers = self.http_headers
        
        # Dune API expects parameters in a specific format
        params = {}
        
        # Add performance parameter if specified
        if self.config.get("performance"):
            params["performance"] = self.config["performance"]
        
        # Convert parameters list to dictionary and handle replication
        query_params = {}
        for param in self.config.get("query_parameters", []):
            key = param["key"]
            value = param["value"]
            
            # If this is a replication key parameter, use the state value if available
            if param.get("replication_key"):
                state_value = self.get_starting_replication_key_value(context)
                if state_value:
                    value = state_value
            
            query_params[key] = value
        
        if query_params:
            params["query_parameters"] = query_params
        
        self.logger.info(f"Request URL: {url}")
        self.logger.info(f"Request Headers: {headers}")
        self.logger.info(f"Request Body: {params}")
        
        request = requests.Request(
            method=http_method,
            url=url,
            headers=headers,
            json=params  # Send parameters in request body
        )
        return request.prepare()
    
    def get_next_page_token(self, response: requests.Response, previous_token: Optional[Any]) -> Optional[Any]:
        """No pagination in Dune query results."""
        return None
    
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.
        
        Args:
            response: The HTTP ``requests.Response`` object.
            
        Yields:
            Each record from the source.
        """
        # Start query execution
        execution_data = response.json()
        execution_id = execution_data["execution_id"]
        
        # Poll until query is complete
        while True:
            status_response = requests.get(
                f"{self.url_base}/execution/{execution_id}/status",
                headers=self.http_headers
            )
            status_data = status_response.json()
            
            if status_data["state"] == "QUERY_STATE_COMPLETED":
                break
            elif status_data["state"] in ["QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"]:
                raise FatalAPIError(f"Query execution failed: {status_data.get('error')}")
            
            time.sleep(2)  # Wait before polling again
        
        # Get results
        results_response = requests.get(
            f"{self.url_base}/execution/{execution_id}/results",
            headers=self.http_headers
        )
        results_data = results_response.json()
        
        # Add execution metadata and replication key to each row
        for row in results_data["result"]["rows"]:
            row["execution_id"] = execution_id
            row["execution_time"] = results_data.get("execution_ended_at")
            
            # Add replication key value if configured
            if self.replication_key:
                # Find the replication key parameter value
                for param in self.config.get("query_parameters", []):
                    if param.get("replication_key"):
                        row[self.replication_key] = param["value"]
                        break
            
            yield row