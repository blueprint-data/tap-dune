"""Dune tap class."""

from typing import List

from singer_sdk import Tap
from singer_sdk import typing as th

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
            required=True,
            description="JSON Schema definition for the query result fields"
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
                if replication_key in self.config["schema"]["properties"]:
                    replication_key_format = self.config["schema"]["properties"][replication_key].get("format")
                break

        # Create the schema with execution metadata and query result fields
        schema = {
            "type": "object",
            "properties": {
                # Add execution metadata fields
                "execution_id": {"type": "string"},
                "execution_time": {"type": "string", "format": "date-time"},
                # Add query result fields from config
                **self.config["schema"]["properties"]
            }
        }

        # Add replication key field if not already in schema
        if replication_key and replication_key not in schema["properties"]:
            schema["properties"][replication_key] = {
                "type": "string",
                "format": replication_key_format or "date-time"  # Default to date-time if format not specified
            }
        
        return [
            DuneQueryStream(
                tap=self,
                name="dune_query",
                query_id=self.config["query_id"],
                schema=schema
            )
        ]