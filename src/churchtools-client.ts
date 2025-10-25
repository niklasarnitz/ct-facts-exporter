import { churchtoolsClient } from "@churchtools/churchtools-client";
import * as axiosCookieJarSupport from "axios-cookiejar-support";
import { CookieJar } from "tough-cookie";

// Set up cookie jar for Node.js session handling
const cookieJar = new CookieJar();
churchtoolsClient.setCookieJar(axiosCookieJarSupport.wrapper, cookieJar);

export interface CTFact {
  id: number;
  name: string;
  nameTranslated: string;
  type: "number" | "select";
  unit?: string;
  sortKey: number;
}

export interface CTEvent {
  id: number;
  name: string;
  startDate: string;
  endDate?: string;
  calendarId?: number;
}

export interface CTEventFact {
  eventId: number;
  factId: number;
  value: number | string;
  modifiedDate?: string;
}

export interface CTMasterData {
  facts?: CTFact[];
}

let isAuthenticated = false;

export const ctClient = {
  async authenticate(baseUrl: string, username: string, password: string) {
    churchtoolsClient.setBaseUrl(baseUrl);

    try {
      await churchtoolsClient.post("/login", { username, password });

      // Verify authentication
      await churchtoolsClient.get("/whoami");

      isAuthenticated = true;
      console.log("Successfully authenticated with ChurchTools");
    } catch (error) {
      isAuthenticated = false;
      throw new Error(`Authentication failed: ${error}`);
    }
  },

  isAuthenticated() {
    return isAuthenticated;
  },

  async getMasterData(): Promise<CTMasterData> {
    if (!isAuthenticated) {
      throw new Error("Not authenticated");
    }

    try {
      const response = await churchtoolsClient.get<{ data: CTMasterData }>(
        "/event/masterdata"
      );
      const data = this.unwrap(response) as CTMasterData;
      return data || { facts: [] };
    } catch (error) {
      console.error("Error fetching master data:", error);
      throw error;
    }
  },

  async getEvents(
    yearOrFrom?: number | string,
    to?: string
  ): Promise<CTEvent[]> {
    if (!isAuthenticated) {
      throw new Error("Not authenticated");
    }

    try {
      const params = new URLSearchParams();

      if (typeof yearOrFrom === "number") {
        // Year provided
        params.append("from", `${yearOrFrom}-01-01`);
        params.append("to", `${yearOrFrom}-12-31`);
      } else if (typeof yearOrFrom === "string" && to) {
        // Date range provided
        params.append("from", yearOrFrom);
        params.append("to", to);
      }

      const url = `/events${params.toString() ? `?${params.toString()}` : ""}`;
      const response = await churchtoolsClient.get<
        { data: CTEvent[] } | CTEvent[]
      >(url);
      const data = this.unwrap(response) as CTEvent[];
      return data || [];
    } catch (error) {
      console.error("Error fetching events:", error);
      throw error;
    }
  },

  async getEventFacts(eventId: number): Promise<CTEventFact[]> {
    if (!isAuthenticated) {
      throw new Error("Not authenticated");
    }

    try {
      const response = await churchtoolsClient.get<{ data: CTEventFact[] }>(
        `/events/${eventId}/facts`
      );
      const data = this.unwrap(response) as CTEventFact[];
      return data || [];
    } catch (error) {
      console.error(`Error fetching facts for event ${eventId}:`, error);
      // Return empty array instead of throwing to continue with other events
      return [];
    }
  },

  async getAllFactsForYear(year: number): Promise<{
    events: CTEvent[];
    allFacts: CTEventFact[];
  }> {
    const events = await this.getEvents(year);

    // Fetch facts for all events in parallel (in batches to avoid rate limiting)
    const batchSize = 10;
    const allFacts: CTEventFact[] = [];

    for (let i = 0; i < events.length; i += batchSize) {
      const batch = events.slice(i, i + batchSize);
      const factPromises = batch.map((event) =>
        event.id ? this.getEventFacts(event.id) : Promise.resolve([])
      );

      const eventFactsArrays = await Promise.all(factPromises);
      allFacts.push(...eventFactsArrays.flat());
    }

    return { events, allFacts };
  },

  async getAllFactsForYearWithDelay(
    year: number,
    delayMs: number = 10
  ): Promise<{
    events: CTEvent[];
    allFacts: CTEventFact[];
  }> {
    const events = await this.getEvents(year);
    const allFacts: CTEventFact[] = [];

    console.log(
      `Fetching facts for ${events.length} events with ${delayMs}ms delay...`
    );

    for (let i = 0; i < events.length; i++) {
      const event = events[i];
      if (event.id) {
        const facts = await this.getEventFacts(event.id);
        allFacts.push(...facts);

        // Add delay between requests to avoid rate limiting
        if (i < events.length - 1) {
          await new Promise((resolve) => setTimeout(resolve, delayMs));
        }

        // Log progress every 10 events
        if ((i + 1) % 10 === 0) {
          console.log(`  Processed ${i + 1}/${events.length} events...`);
        }
      }
    }

    console.log(`Completed fetching facts for ${events.length} events`);
    return { events, allFacts };
  },

  async getAllFactsForDateRange(
    from: string,
    to: string
  ): Promise<{
    events: CTEvent[];
    allFacts: CTEventFact[];
  }> {
    const events = await this.getEvents(from, to);

    // Fetch facts for all events in parallel (in batches to avoid rate limiting)
    const batchSize = 10;
    const allFacts: CTEventFact[] = [];

    for (let i = 0; i < events.length; i += batchSize) {
      const batch = events.slice(i, i + batchSize);
      const factPromises = batch.map((event) =>
        event.id ? this.getEventFacts(event.id) : Promise.resolve([])
      );

      const eventFactsArrays = await Promise.all(factPromises);
      allFacts.push(...eventFactsArrays.flat());
    }

    return { events, allFacts };
  },

  unwrap(res: any): any {
    if (res == null) return null;
    try {
      if (typeof res === "object" && res !== null && "data" in res) {
        return res.data;
      }
    } catch (e) {
      // ignore
    }
    return res;
  },
};
