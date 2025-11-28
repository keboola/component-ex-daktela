# Daktela Extractor Component

Keboola extractor component for Daktela CRM/Contact Center system. Extracts data via Daktela API v6.

## Description

This component extracts data from Daktela CRM/Contact Center system and loads it into Keboola Connection. It is designed as a drop-in replacement for the original `revolt-bi.app-daktela` component with identical output format.

## Supported Tables

The extractor supports the following Daktela entities:

| Table | Description |
|-------|-------------|
| `activities` | User activities and interactions |
| `activities_statuses` | Activity status history |
| `activities_call` | Call-specific activity data (requires activities) |
| `activities_email` | Email-specific activity data (requires activities) |
| `activities_chat` | Chat-specific activity data (requires activities) |
| `contacts` | CRM contacts |
| `tickets` | Support tickets |
| `users` | Daktela users/agents |
| `queues` | Call queues |
| `campaigns` | Outbound campaigns |
| `accounts` | Customer accounts |
| `statuses` | Status definitions |
| `categories` | Category definitions |
| `records` | Call recordings metadata |

## Configuration

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `username` | string | Yes | Daktela account username |
| `#password` | string | Yes | Daktela account password (encrypted) |
| `url` | string | No* | Full Daktela instance URL (e.g., `https://mycompany.daktela.com`) |
| `server` | string | No* | Daktela server name (e.g., `mycompany`) |
| `from` | string | Yes | Start date for data extraction |
| `to` | string | Yes | End date for data extraction |
| `tables` | string | Yes | Comma-separated list of tables to extract |
| `incremental` | boolean | No | Enable incremental loading (default: false) |
| `debug` | boolean | No | Enable debug logging (default: false) |

*Either `url` or `server` must be provided.

### Date Format

The `from` and `to` parameters support the following formats:

- `today` or `0` - Current datetime minus 30 minutes
- Negative integer (e.g., `-7`) - Today minus N days
- Date string `YYYY-MM-DD` - Explicit date

### Example Configuration

```json
{
  "username": "api_user",
  "#password": "your_password",
  "server": "mycompany",
  "from": "-7",
  "to": "0",
  "tables": "activities,contacts,tickets,users",
  "incremental": true
}
```

## Features

- **Incremental and Full Load**: Choose between appending new data or replacing existing data
- **Flexible Date Filtering**: Use relative dates (e.g., `-7` for last 7 days) or explicit dates
- **Automatic Pagination**: Handles large datasets with configurable batch sizes
- **Retry Logic**: Exponential backoff retry strategy for reliability (up to 8 retries)
- **Parent-Child Relationships**: Automatically handles dependent tables (e.g., activities → activities_email)
- **Data Integrity**: MD5-based compound ID generation for consistent primary keys
- **Multi-Instance Support**: Server prefix on key columns for data from multiple Daktela instances

## Output

The component outputs CSV files with the following characteristics:

- One file per table: `{server}_{table_name}.csv`
- Manifest files with column definitions and primary keys
- Server prefix added to ID columns for multi-instance support
- Compound primary key (`id`) generated using MD5 hash

### Output Columns

Each output table includes:

1. `server` - Server name identifier
2. `id` - Compound primary key (MD5 hash)
3. Table-specific columns as defined in configuration

## Development

### Local Development

```bash
# Clone the repository
git clone https://github.com/keboola/component-ex-daktela.git
cd component-ex-daktela

# Install dependencies
pip install -r requirements.txt

# Run tests
python -m pytest tests/

# Run linting
flake8 src/ tests/
```

### Running Locally

```bash
# Set up data directory
export KBC_DATADIR=/path/to/data

# Run the component
python -m src.component
```

## API Documentation

- [Daktela V6 API Documentation](https://docs.daktela.com/en/other/working-version/daktela-v6-api-developing-a-custom-agent-interface)
- [Daktela REST API Guide](https://docs.daktela.com/en/integrations/working-version/rest-api)

## License

MIT License
