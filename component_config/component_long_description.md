## Daktela Extractor

Extracts data from Daktela CRM/Contact Center system via API v6.

### Supported Tables

The extractor supports the following Daktela entities:

- **activities** - User activities and interactions
- **activities_statuses** - Activity status history
- **activities_call** - Call-specific activity data
- **activities_email** - Email-specific activity data
- **activities_chat** - Chat-specific activity data
- **contacts** - CRM contacts
- **tickets** - Support tickets
- **users** - Daktela users/agents
- **queues** - Call queues
- **campaigns** - Outbound campaigns
- **accounts** - Customer accounts
- **statuses** - Status definitions
- **categories** - Category definitions
- **records** - Call recordings metadata

### Features

- Incremental and full load modes
- Flexible date range filtering (relative dates like '-7' for last 7 days, or explicit YYYY-MM-DD)
- Automatic pagination for large datasets
- Retry logic with exponential backoff for reliability
- Parent-child table relationships (e.g., activities → activities_email)
- MD5-based compound ID generation for data integrity
- Server prefix on key columns for multi-instance support

### Configuration

Provide your Daktela credentials (username and password), specify either the full URL or server name, set the date range, and select which tables to extract.

### Migration from revolt-bi.app-daktela

This component is designed as a drop-in replacement for the original `revolt-bi.app-daktela` component with identical output format
