import { Hono } from "hono";
import { config } from "dotenv";
import { ctClient } from "./churchtools-client";
import { dataSyncService } from "./sync";
import { grafanaHandlers } from "./grafana";
import { database } from "./db";

// Load environment variables
config();

const app = new Hono();

// Grafana JSON datasource endpoints
app.get("/", grafanaHandlers.root);
app.post("/metrics", grafanaHandlers.metrics);
app.post("/query", grafanaHandlers.query);
app.post("/annotations", grafanaHandlers.annotations);
app.post("/tag-keys", grafanaHandlers.tagKeys);
app.post("/tag-values", grafanaHandlers.tagValues);

// Health check endpoint
app.get("/health", (c) => {
  const lastSync = database.getLastSyncTime();
  return c.json({
    status: "ok",
    authenticated: ctClient.isAuthenticated(),
    lastSync: lastSync?.toISOString() || null,
  });
});

// Manual sync trigger endpoint (useful for testing)
app.post("/sync", async (c) => {
  try {
    await dataSyncService.syncData();
    return c.json({ status: "success", message: "Sync completed" });
  } catch (error) {
    return c.json({ status: "error", message: String(error) }, 500);
  }
});

// Initialize the application
async function initialize() {
  const { CT_BASE_URL, CT_USERNAME, CT_PASSWORD, PORT = "3000" } = process.env;

  if (!CT_BASE_URL || !CT_USERNAME || !CT_PASSWORD) {
    throw new Error(
      "Missing required environment variables: CT_BASE_URL, CT_USERNAME, CT_PASSWORD"
    );
  }

  try {
    console.log("Authenticating with ChurchTools...");
    await ctClient.authenticate(CT_BASE_URL, CT_USERNAME, CT_PASSWORD);

    console.log("Starting initial data sync...");
    await dataSyncService.performInitialSync();

    console.log("Starting cron job for hourly sync...");
    dataSyncService.startCronJob();

    console.log(`Server is running on port ${PORT}`);
    console.log(`Grafana JSON datasource URL: http://localhost:${PORT}`);
  } catch (error) {
    console.error("Failed to initialize application:", error);
    throw error;
  }
}

// Graceful shutdown
process.on("SIGINT", () => {
  console.log("Shutting down...");
  dataSyncService.stopCronJob();
  database.close();
  process.exit(0);
});

process.on("SIGTERM", () => {
  console.log("Shutting down...");
  dataSyncService.stopCronJob();
  database.close();
  process.exit(0);
});

// Start the application
initialize().catch((error) => {
  console.error("Failed to start application:", error);
  process.exit(1);
});

export { app };
