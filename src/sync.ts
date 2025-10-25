import cron, { ScheduledTask } from "node-cron";
import { ctClient } from "./churchtools-client";
import { database } from "./db";

export class DataSyncService {
  private syncTask: ScheduledTask | null = null;
  private isSyncing = false;

  async syncData() {
    if (this.isSyncing) {
      console.log("Sync already in progress, skipping...");
      return;
    }

    this.isSyncing = true;
    console.log("Starting data sync...");

    try {
      // Fetch master data (facts definitions)
      console.log("Fetching master data...");
      const masterData = await ctClient.getMasterData();

      if (masterData.facts) {
        console.log(`Syncing ${masterData.facts.length} facts...`);
        for (const fact of masterData.facts) {
          database.upsertFact({
            id: fact.id,
            name: fact.name,
            name_translated: fact.nameTranslated,
            type: fact.type,
            unit: fact.unit,
            sort_key: fact.sortKey,
          });
        }
      }

      // Fetch events and facts for current and previous month
      const now = new Date();
      const currentMonth = new Date(now.getFullYear(), now.getMonth(), 1);
      const previousMonth = new Date(now.getFullYear(), now.getMonth() - 1, 1);
      const nextMonth = new Date(now.getFullYear(), now.getMonth() + 1, 1);

      const from = previousMonth.toISOString().split("T")[0];
      const to = nextMonth.toISOString().split("T")[0];

      console.log(`Fetching events and facts from ${from} to ${to}...`);
      const { events, allFacts } = await ctClient.getAllFactsForDateRange(
        from,
        to
      );

      // Create a map of events by ID for quick lookup
      const eventsMap = new Map(events.map((e) => [e.id, e]));

      console.log(`Syncing ${events.length} events...`);
      for (const event of events) {
        if (event.id) {
          database.insertEvent({
            id: event.id,
            name: event.name,
            start_date: event.startDate,
            end_date: event.endDate,
            calendar_id: event.calendarId,
          });
        }
      }

      console.log(`Syncing ${allFacts.length} event facts...`);
      for (const fact of allFacts) {
        const event = eventsMap.get(fact.eventId);
        database.insertEventFact({
          event_id: fact.eventId,
          fact_id: fact.factId,
          value: fact.value,
          modified_date: fact.modifiedDate,
          event_name: event?.name,
        });
      }

      const lastSync = database.getLastSyncTime();
      console.log(`Data sync completed successfully at ${lastSync}`);
    } catch (error) {
      console.error("Error during data sync:", error);
      throw error;
    } finally {
      this.isSyncing = false;
    }
  }

  async syncYear(year: number) {
    if (this.isSyncing) {
      throw new Error("Sync already in progress");
    }

    this.isSyncing = true;
    console.log(`Starting full year sync for ${year}...`);

    try {
      // Fetch master data first
      console.log("Fetching master data...");
      const masterData = await ctClient.getMasterData();

      if (masterData.facts) {
        console.log(`Syncing ${masterData.facts.length} facts...`);
        for (const fact of masterData.facts) {
          database.upsertFact({
            id: fact.id,
            name: fact.name,
            name_translated: fact.nameTranslated,
            type: fact.type,
            unit: fact.unit,
            sort_key: fact.sortKey,
          });
        }
      }

      console.log(`Fetching events and facts for year ${year}...`);
      const { events, allFacts } = await ctClient.getAllFactsForYearWithDelay(
        year,
        10
      );

      // Create a map of events by ID for quick lookup
      const eventsMap = new Map(events.map((e) => [e.id, e]));

      console.log(`Syncing ${events.length} events for ${year}...`);
      for (const event of events) {
        if (event.id) {
          database.insertEvent({
            id: event.id,
            name: event.name,
            start_date: event.startDate,
            end_date: event.endDate,
            calendar_id: event.calendarId,
          });
        }
      }

      console.log(`Syncing ${allFacts.length} event facts for ${year}...`);
      for (const fact of allFacts) {
        const event = eventsMap.get(fact.eventId);
        database.insertEventFact({
          event_id: fact.eventId,
          fact_id: fact.factId,
          value: fact.value,
          modified_date: fact.modifiedDate,
          event_name: event?.name,
        });
      }

      const lastSync = database.getLastSyncTime();
      console.log(`Year ${year} sync completed successfully at ${lastSync}`);
      return { events: events.length, facts: allFacts.length };
    } catch (error) {
      console.error(`Error during year ${year} sync:`, error);
      throw error;
    } finally {
      this.isSyncing = false;
    }
  }

  startCronJob() {
    // Run every hour at minute 0
    this.syncTask = cron.schedule("0 * * * *", async () => {
      console.log("Hourly sync triggered");
      try {
        await this.syncData();
      } catch (error) {
        console.error("Cron sync failed:", error);
      }
    });

    console.log("Cron job started: Data will sync every hour at minute 0");
  }

  stopCronJob() {
    if (this.syncTask) {
      this.syncTask.stop();
      this.syncTask = null;
      console.log("Cron job stopped");
    }
  }

  async performInitialSync() {
    console.log("Performing initial data sync...");
    const lastSync = database.getLastSyncTime();

    if (!lastSync) {
      console.log("No previous sync found, starting full sync...");
      await this.syncData();
    } else {
      const hoursSinceLastSync =
        (Date.now() - lastSync.getTime()) / (1000 * 60 * 60);
      console.log(`Last sync was ${hoursSinceLastSync.toFixed(2)} hours ago`);

      if (hoursSinceLastSync > 1) {
        console.log("Last sync was more than 1 hour ago, starting sync...");
        await this.syncData();
      } else {
        console.log("Recent sync found, skipping initial sync");
      }
    }
  }
}

export const dataSyncService = new DataSyncService();
