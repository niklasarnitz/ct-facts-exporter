# ChurchTools Facts Exporter

A Grafana JSON datasource that exports ChurchTools event facts for visualization and reporting.

## Features

- Provides ChurchTools event data via Grafana JSON datasource API
- Automatic data synchronization with configurable intervals
- Multiple aggregation levels (raw, monthly, yearly)
- Support for filtering by event types
- Real-time health monitoring

## Deployment

### Environment Variables

Set the following environment variables:

```bash
CT_BASE_URL=https://your-church.church.tools
CT_LOGIN_TOKEN=your-churchtools-login-token
PORT=3000  # Optional, defaults to 3000
```

### Railway/Nixpacks Deployment

This project is configured for easy deployment on Railway or other Nixpacks-compatible platforms:

1. Connect your repository to Railway
2. Set the required environment variables
3. Deploy automatically

### Docker Deployment

```bash
# Build the image
docker build -t ct-facts-exporter .

# Run the container
docker run -p 3000:3000 \
  -e CT_BASE_URL=https://your-church.church.tools \
  -e CT_LOGIN_TOKEN=your-token \
  ct-facts-exporter
```

### Local Development

```bash
# Install dependencies
bun install

# Copy environment file and configure
cp .env.example .env
# Edit .env with your settings

# Run development server
bun run dev
```

## API Endpoints

- `GET /health` - Health check and authentication status
- `POST /metrics` - Grafana datasource metrics discovery
- `POST /query` - Grafana datasource query endpoint
- `POST /sync` - Manual data synchronization trigger
- `POST /sync-year/:year` - Sync specific year data

## Grafana Configuration

Add as a JSON datasource in Grafana:

- URL: `http://your-server:3000`
- Method: Server (default)

## License

MIT
