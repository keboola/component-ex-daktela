## Daktela Extractor

Extracts data from Daktela CRM/Contact Center system via API v6. Supports **36 tables** covering all major Daktela entities.

### Supported Tables

#### Core Tables (27)
- **accounts** - CRM customer accounts
- **activities** - User activities and interactions
- **calls** - Call records with duration, disposition, recordings
- **campaigns** - Outbound campaigns
- **categories** - Category definitions
- **chats** - Chat conversations
- **contacts** - CRM contacts
- **devices** - SIP/WebRTC devices
- **emails** - Email messages with full headers
- **fields** - Custom field definitions
- **files** - File attachments
- **forms** - Web forms
- **groups** - User groups
- **holidays** - Holiday calendar
- **notes** - Notes and comments
- **pauses** - Pause reason definitions
- **profiles** - User profiles and permissions
- **queues** - Call/ticket queues
- **recordings** - Call recording metadata
- **records** - Custom records
- **schedules** - Work schedules
- **skills** - User skills
- **sms** - SMS messages
- **statuses** - Status definitions
- **templates** - Message templates
- **tickets** - Support tickets
- **users** - Daktela users/agents

#### Specialized Tables (9)
- **accounts_custom_fields** - Custom fields for accounts
- **activities_call** - Call-specific activity details (requires activities table)
- **activities_chat** - Chat-specific activity details (requires activities table)
- **activities_email** - Email-specific activity details (requires activities table)
- **activities_sms** - SMS-specific activity details (requires activities table)
- **activities_statuses** - Activity status history
- **contacts_custom_fields** - Custom fields for contacts
- **tickets_categories** - Hierarchical ticket categories
- **users_queues** - User-queue priority assignments

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
