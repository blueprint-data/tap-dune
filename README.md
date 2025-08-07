# tap-dune

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://hub.meltano.com/spec).

This tap:
- Pulls data from the [Dune Analytics API](https://dune.com/docs/api/)
- Extracts data from specified Dune queries
- Produces [Singer](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md) formatted data following the Singer spec
- Supports incremental replication using query parameters
- Automatically infers schema from query results

## Installation

```bash
pipx install poetry
git clone https://github.com/your-username/tap-dune.git
cd tap-dune
poetry install
```

## Configuration

### Accepted Config Options

A full list of supported settings and capabilities is available by running:

```bash
poetry run tap-dune --about
```

### Config File Setup

1. Copy the example config file:
   ```bash
   cp config.json.example config.json
   ```

2. Edit `config.json` with your settings:

```json
{
    "api_key": "YOUR_DUNE_API_KEY",
    "query_id": "YOUR_QUERY_ID",
    "performance": "medium",
    "query_parameters": [
        {
            "key": "date_from",
            "value": "2025-08-01",
            "replication_key": true
        }
    ]
}
```

### Configuration Fields

| Field | Required | Description |
|-------|----------|-------------|
| `api_key` | Yes | Your Dune Analytics API key |
| `query_id` | Yes | The ID of the Dune query to execute |
| `performance` | No | Query execution performance tier: 'medium' (10 credits) or 'large' (20 credits). Defaults to 'medium' |
| `query_parameters` | No | Array of parameters to pass to your Dune query |
| `schema` | No | Optional: JSON Schema definition of your query's output fields. If not provided, schema will be inferred from query results |

#### Query Parameters

Each query parameter object can have:
- `key`: Parameter name in your Dune query
- `value`: Parameter value
- `replication_key`: Set to `true` for the parameter that should be used for incremental replication

#### Schema Configuration

The schema can be:
1. Automatically inferred from query results (recommended)
2. Explicitly defined in the config file

When automatically inferring the schema:
- The tap will execute the query once to get sample data
- Data types are detected based on the values in the results
- Special formats like dates and timestamps are automatically recognized
- Null values are handled by looking at other rows to determine the correct type
- If a type cannot be determined, it defaults to string

If you need to explicitly define the schema, each field should specify:
- `type`: The data type ('string', 'number', 'integer', 'boolean', 'object', 'array')
- `format` (optional): Special format for string fields (e.g., 'date', 'date-time')

Example of explicit schema configuration:
```json
{
    "schema": {
        "properties": {
            "day": {"type": "string", "format": "date"},
            "network": {"type": "string"},
            "total_mana": {"type": "number"},
            "total_usd": {"type": "number"}
        }
    }
}
```

### Source Authentication and Authorization

1. Visit [Dune Analytics](https://dune.com)
2. Create an account and obtain an API key
3. Add the API key to your config file

## Usage

### Basic Usage

1. Generate a catalog file:
   ```bash
   poetry run tap-dune --config config.json --discover > catalog.json
   ```

2. Run the tap:
   ```bash
   poetry run tap-dune --config config.json --catalog catalog.json
   ```

### Incremental Replication

To use incremental replication:

1. Mark one of your query parameters with `"replication_key": true`
2. Ensure the parameter value is in a format that can be ordered (e.g., dates, timestamps)
3. The tap will track the last value processed and resume from there in subsequent runs

### Pipeline Usage

You can easily run `tap-dune` in a pipeline using [Meltano](https://meltano.com/) or any other Singer-compatible tool.

Example with `target-jsonl`:
```bash
poetry run tap-dune --config config.json --catalog catalog.json | target-jsonl
```

## Development

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Testing

```bash
poetry run pytest
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to develop your own taps and targets.