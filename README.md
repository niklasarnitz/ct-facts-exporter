# ChurchTools Facts Grafana JSON Datasource

A Grafana JSON datasource provider for ChurchTools facts. This application fetches numeric facts from ChurchTools, caches them in a SQLite database, and exposes them via the Grafana JSON datasource API.

## Features

- 🔐 Authentication with ChurchTools using username/password
- ⏰ Hourly automatic data synchronization via cron
- 💾 SQLite database caching to minimize ChurchTools API requests
- 📊 Full Grafana JSON datasource API implementation
- 🔢 Support for numeric ChurchTools facts with units
- 📅 Time-range based queries for event facts
- 🏷️ Event name filtering - track "Gottesdienst" and "Gottesdienst mit Abendmahl" separately
- 📈 Multiple aggregations:
  - **Raw Data**: Individual fact values by event date
  - **Monthly Sum**: Aggregated values per month
  - **Yearly Sum**: Total sum per year
  - **Yearly Mean**: Average value per year
- 🔍 Filter metrics by event name (include/exclude specific event types)

## Prerequisites

- [Bun](https://bun.sh) runtime
- ChurchTools account with API access
- Grafana instance (for visualization)

## Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd ct-facts-exporter
```

2. Install dependencies:

```bash
bun install
```

3. Create a `.env` file from the example:

```bash
cp .env.example .env
```

4. Configure your ChurchTools credentials in `.env`:

```env
CT_BASE_URL=https://your-church.church.tools
CT_USERNAME=your-username
CT_PASSWORD=your-password
PORT=3000
```

## Usage

### Development Mode

Run the application in development mode with hot reload:

```bash
bun run dev
```

### Production Mode

Build and run the application:

```bash
bun run src/index.ts
```

## API Endpoints

### Grafana JSON Datasource Endpoints

- `GET /` - Root endpoint, returns datasource information
- `POST /metrics` - Returns available metrics with filtering options
- `POST /query` - Returns time series data for selected metrics
- `POST /annotations` - Returns annotations (not implemented)
- `POST /tag-keys` - Returns tag keys for filtering
- `POST /tag-values` - Returns tag values for filtering

### Additional Endpoints

- `GET /health` - Health check endpoint with sync status
- `POST /sync` - Manually trigger data synchronization

## Database Schema

The application uses SQLite with the following tables:

- **facts** - Stores fact definitions from ChurchTools
- **events** - Stores event information
- **event_facts** - Stores fact values for each event

## Data Synchronization

- The application performs an initial sync on startup
- After the initial sync, data is synchronized hourly at minute 0
- Synchronization fetches data for the current and previous year
- All sync operations are logged to the console

## Grafana Configuration

To use this datasource in Grafana:

1. Install the "JSON API" datasource plugin in Grafana
2. Add a new JSON API datasource
3. Set the URL to `http://localhost:3000` (or your server URL)
4. Save and test the datasource

### Creating a Dashboard

1. Create a new dashboard in Grafana
2. Add a panel
3. Select your ChurchTools Facts datasource
4. In the query editor, select a fact from the dropdown
5. Adjust the time range as needed
6. Configure visualization options

## Development

### Project Structure

```
ct-facts-exporter/
├── src/
│   ├── index.ts              # Main application entry point
│   ├── db.ts                 # SQLite database module
│   ├── churchtools-client.ts # ChurchTools API client
│   ├── sync.ts               # Data synchronization service
│   └── grafana.ts            # Grafana JSON datasource handlers
├── package.json
├── tsconfig.json
├── .env.example
└── README.md
```

### Key Components

- **Database (`db.ts`)**: Manages SQLite database operations and schema
- **ChurchTools Client (`churchtools-client.ts`)**: Handles authentication and API requests to ChurchTools
- **Sync Service (`sync.ts`)**: Manages data synchronization with cron jobs
- **Grafana Handlers (`grafana.ts`)**: Implements Grafana JSON datasource API endpoints

## Troubleshooting

### Authentication Issues

If you encounter authentication errors:

- Verify your ChurchTools credentials in `.env`
- Ensure your ChurchTools user has API access permissions
- Check that the base URL is correct (include `https://`)

### Data Not Showing

If data doesn't appear in Grafana:

- Check the `/health` endpoint to verify sync status
- Manually trigger a sync using `POST /sync`
- Check the console logs for errors during synchronization
- Verify that your ChurchTools instance has events with facts

### Database Issues

The SQLite database file (`data.db`) is created automatically. If you encounter database errors:

- Delete the `data.db` file and restart the application
- Check file permissions in the application directory

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
