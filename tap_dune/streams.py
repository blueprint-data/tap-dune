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

    def __init__(self, tap: Any, name: str, query_id: str, schema: dict = None, replication_key: str = None, replication_key_type: str = None, **kwargs):
        """Initialize the stream.
        
        Args:
            tap: The parent tap object
            name: The stream name
            query_id: The Dune query ID
            schema: The stream schema (from query results)
            replication_key: The replication key field name
            replication_key_type: The type of the replication key (string, integer, number, date, date-time)
        """
        self.query_id = query_id
        self.replication_key = replication_key
        self.replication_key_type = replication_key_type
        
    def get_replication_key_value(self, context: Optional[dict]) -> Any:
        """Get the current replication key value from query parameters.
        
        Args:
            context: Stream partition or context dictionary.
            
        Returns:
            The current value of the replication key.
        """
        if not self.replication_key:
            return None
            
        # Get the current value from state if available
        current_value = self.get_starting_replication_key_value(context)
        if current_value:
            return current_value
            
        # Otherwise get the initial value from query parameters
        for param in self.config.get("query_parameters", []):
            if param.get("replication_key"):
                value = param["value"]
                # Convert value based on type if specified
                param_type = param.get("type", "string")
                if param_type == "integer":
                    return int(value)
                elif param_type == "number":
                    return float(value)
                return value
                
        return None
        
    def __init__(self, tap: Any, name: str, query_id: str, schema: dict = None, replication_key: str = None, replication_key_type: str = None, **kwargs):
        """Initialize the stream.
        
        Args:
            tap: The parent tap object
            name: The stream name
            query_id: The Dune query ID
            schema: The stream schema (from query results)
            replication_key: The replication key field name
            replication_key_type: The type of the replication key (string, integer, number, date, date-time)
        """
        self.query_id = query_id
        self.replication_key = replication_key
        self.replication_key_type = replication_key_type
        
        # Set up replication key type
        if replication_key_type in ["date", "date-time"]:
            self.replication_key_jsonschema = {"type": "string", "format": replication_key_type}
        elif replication_key_type in ["integer", "number"]:
            self.replication_key_jsonschema = {"type": replication_key_type}
        else:
            self.replication_key_jsonschema = {"type": "string"}
        
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

        # Set replication key type based on parameter configuration
        if self.replication_key:
            if replication_key_type in ["date", "date-time"]:
                self.replication_key_jsonschema = {"type": "string", "format": replication_key_type}
            elif replication_key_type in ["integer", "number"]:
                self.replication_key_jsonschema = {"type": replication_key_type}
            else:
                # Default to string type if no type is specified or type is string
                self.replication_key_jsonschema = {"type": "string"}

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
        
        # Log request details without sensitive info
        self.logger.info("Making request to Dune API", extra={
            "url": url,
            "params": {
                **params,
                "query_parameters": "***" if "query_parameters" in params else None
            }
        })
        
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
        
    def get_replication_key_value(self, context: Optional[dict]) -> Any:
        """Get the current replication key value from query parameters.
        
        Args:
            context: Stream partition or context dictionary.
            
        Returns:
            The current value of the replication key.
        """
        if not self.replication_key:
            return None
            
        # Get the current value from state if available
        current_value = self.get_starting_replication_key_value(context)
        if current_value:
            return current_value
            
        # Otherwise get the initial value from query parameters
        for param in self.config.get("query_parameters", []):
            if param.get("replication_key"):
                value = param["value"]
                # Convert value based on type if specified
                param_type = param.get("type", "string")
                if param_type == "integer":
                    return int(value)
                elif param_type == "number":
                    return float(value)
                return value
                
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
            
            # Add replication key to row for SDK but filter it out in post_process
            if self.replication_key:
                # Find the replication key parameter value
                for param in self.config.get("query_parameters", []):
                    if param.get("replication_key"):
                        value = param["value"]
                        # Convert value based on type if specified
                        param_type = param.get("type", "string")
                        if param_type == "integer":
                            value = int(value)
                        elif param_type == "number":
                            value = float(value)
                        row[self.replication_key] = value
                        break
            
            yield row
            
    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> dict:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Value used to get the next page of records.

        Returns:
            A dictionary of URL query parameters.
        """
        params = {
            "performance": self.config.get("performance", "medium")
        }

        # Add query parameters if any
        query_params = {}
        for param in self.config.get("query_parameters", []):
            key = param["key"]
            value = param["value"]
            
            # If this is a replication key parameter, use the state value if available
            if param.get("replication_key"):
                starting_value = self.get_starting_replication_key_value(context)
                if starting_value:
                    value = starting_value
            
            query_params[key] = value
        
        if query_params:
            params["query_parameters"] = query_params

        return params

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of record-type dictionary objects.
        
        Args:
            context: Stream partition or context dictionary.
            
        Yields:
            One item per (possibly processed) record in the API.
        """
        # Get the replication key field mapping if any
        replication_field = None
        if self.replication_key:
            for param in self.config.get("query_parameters", []):
                if param.get("replication_key"):
                    replication_field = param.get("replication_key_field")
                    break

        # Process records from the API
        for row in super().get_records(context):
            # Map the replication key field from results to the parameter key
            if self.replication_key and replication_field:
                if replication_field not in row:
                    self.logger.warning(
                        f"Replication key field '{replication_field}' not found in record",
                        extra={"record": row}
                    )
                else:
                    # Get the value from the result field
                    value = row[replication_field]
                    # For date fields, ensure correct format
                    if isinstance(value, str) and self.replication_key_type in ["date", "date-time"]:
                        try:
                            from datetime import datetime
                            # Parse the date
                            dt = datetime.strptime(value, "%Y-%m-%d")
                            # Keep original format for 'date' type, use ISO for 'date-time'
                            if self.replication_key_type == "date":
                                value = dt.strftime("%Y-%m-%d")
                            else:
                                value = dt.isoformat()
                        except ValueError:
                            self.logger.warning(
                                f"Could not parse date value '{value}' from field '{replication_field}'",
                                extra={"record": row}
                            )
                    # Add the replication key to the record
                    row[self.replication_key] = value
            
            yield row