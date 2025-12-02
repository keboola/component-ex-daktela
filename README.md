# Daktela Extractor Component

Keboola extractor component for Daktela CRM/Contact Center system. Extracts data via Daktela API v6.

## Description

This component extracts data from Daktela CRM/Contact Center system and loads it into Keboola Connection. It is designed as a drop-in replacement for the original `revolt-bi.app-daktela` component with identical output format.

## Supported Tables

The extractor supports 36 Daktela API v6 entities:

### Core Tables
| Table | Description |
|-------|-------------|
| `accounts` | CRM customer accounts |
| `activities` | User activities and interactions |
| `calls` | Call records |
| `campaigns` | Outbound campaigns |
| `categories` | Category definitions |
| `chats` | Chat conversations |
| `contacts` | CRM contacts |
| `devices` | SIP/WebRTC devices |
| `emails` | Email messages |
| `fields` | Custom field definitions |
| `files` | File attachments |
| `forms` | Web forms |
| `groups` | User groups |
| `holidays` | Holiday calendar |
| `notes` | Notes and comments |
| `pauses` | Pause reason definitions |
| `profiles` | User profiles |
| `queues` | Call/ticket queues |
| `recordings` | Call recording metadata |
| `records` | Custom records |
| `schedules` | Work schedules |
| `skills` | User skills |
| `sms` | SMS messages |
| `statuses` | Status definitions |
| `templates` | Message templates |
| `tickets` | Support tickets |
| `users` | Daktela users/agents |

### Specialized Tables
| Table | Description |
|-------|-------------|
| `accounts_custom_fields` | Custom fields for accounts |
| `activities_call` | Call-specific activity data (requires activities) |
| `activities_chat` | Chat-specific activity data (requires activities) |
| `activities_email` | Email-specific activity data (requires activities) |
| `activities_sms` | SMS-specific activity data (requires activities) |
| `activities_statuses` | Activity status history |
| `contacts_custom_fields` | Custom fields for contacts |
| `tickets_categories` | Ticket categories |
| `users_queues` | User-queue assignments |

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

- **Memory-Efficient Streaming**: Handles millions of records without memory issues using async generators
- **Incremental and Full Load**: Choose between appending new data or replacing existing data
- **Flexible Date Filtering**: Use relative dates (e.g., `-7` for last 7 days) or explicit dates
- **Automatic Pagination**: Handles large datasets with configurable batch sizes
- **Retry Logic**: Exponential backoff retry strategy for reliability (up to 8 retries)
- **Parent-Child Relationships**: Automatically handles dependent tables (e.g., activities → activities_email)
- **Data Integrity**: MD5-based compound ID generation for consistent primary keys
- **Multi-Instance Support**: Server prefix on key columns for data from multiple Daktela instances
- **Large Dataset Support**: Tested with 3.5M+ records, constant memory usage regardless of dataset size

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

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
