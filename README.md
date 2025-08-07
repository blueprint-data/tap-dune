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
pip install tap-dune
```

## Configuration

### Accepted Config Options

A full list of supported settings and capabilities is available by running:

```bash
tap-dune --about
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
   tap-dune --config config.json --discover > catalog.json
   ```

2. Run the tap:
   ```bash
   tap-dune --config config.json --catalog catalog.json
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
tap-dune --config config.json --catalog catalog.json | target-jsonl
```

## Development

### Initialize your Development Environment

```bash
# Clone the repository
git clone https://github.com/blueprint-data/tap-dune.git
cd tap-dune

# Install Poetry
pipx install poetry

# Install dependencies
poetry install
```

### Development Workflow

This project follows [Semantic Versioning](https://semver.org/) and uses [Conventional Commits](https://www.conventionalcommits.org/) for automatic versioning.

1. Create a feature branch:
   ```bash
   git checkout -b feat/your-feature
   # or
   git checkout -b fix/your-bugfix
   ```

2. Make your changes and commit using conventional commits:
   ```bash
   # For new features
   git commit -m "feat: add new feature X"

   # For bug fixes
   git commit -m "fix: resolve issue with Y"

   # For breaking changes
   git commit -m "feat: redesign API

   BREAKING CHANGE: This changes the API interface"
   ```

   Commit types:
   - `feat`: A new feature (minor version bump)
   - `fix`: A bug fix (patch version bump)
   - `docs`: Documentation only changes
   - `style`: Changes that don't affect the code's meaning
   - `refactor`: Code change that neither fixes a bug nor adds a feature
   - `perf`: Code change that improves performance
   - `test`: Adding missing tests
   - `chore`: Changes to the build process or auxiliary tools
   - `BREAKING CHANGE`: Any change that breaks backward compatibility (major version bump)

3. Run tests:
   ```bash
   poetry run pytest
   ```

4. Create a pull request to main

### Release Process

1. Create a release branch from main:
   ```bash
   git checkout main
   git pull
   git checkout -b release
   ```

2. Push the branch:
   ```bash
   git push -u origin release
   ```

3. The release workflow will automatically:
   - Analyze commits since last release
   - Determine the next version number based on commit types:
     - `fix:` → patch version (1.0.0 → 1.0.1)
     - `feat:` → minor version (1.0.0 → 1.1.0)
     - `BREAKING CHANGE:` → major version (1.0.0 → 2.0.0)
   - Update CHANGELOG.md
   - Create a git tag with the new version
   - Create a GitHub release
   - Build and publish to PyPI

4. After successful release:
   - Create a PR from the release branch to main
   - This PR will contain all the version updates (CHANGELOG.md, version number)
   - Merge to keep main up-to-date with the latest release

5. Clean up:
   ```bash
   git checkout main
   git pull
   git branch -d release
   ```

### Testing

```bash
poetry run pytest
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to develop your own taps and targets.