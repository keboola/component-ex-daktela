## Configuration

This extractor connects to your Daktela instance and extracts data from selected tables.

### Required Settings

- **Username** - Your Daktela API username
- **Password** - Your Daktela API password (securely encrypted)
- **Server or URL** - Either provide your server name (e.g., `mycompany`) or full URL (e.g., `https://mycompany.daktela.com`)
- **Date Range** - Define the extraction period using flexible date formats
- **Tables** - Select which tables to extract (36 tables available)

### Date Format Options

- `today` or `0` - Current time minus 30 minutes
- `-7` - Last 7 days (any negative number)
- `2024-01-01` - Specific date (YYYY-MM-DD format)

### Advanced Options

- **Incremental Load** - Append new data instead of replacing (useful for ongoing extractions)
- **Debug Mode** - Enable verbose logging for troubleshooting

### Custom Tables

You can also enter custom table names not in the predefined list if your Daktela instance has custom entities.