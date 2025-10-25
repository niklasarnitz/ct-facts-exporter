import { churchtoolsClient } from "@churchtools/churchtools-client";

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

  async getEvents(year?: number): Promise<CTEvent[]> {
    if (!isAuthenticated) {
      throw new Error("Not authenticated");
    }

    try {
      const params = new URLSearchParams();

      if (year) {
        params.append("from", `${year}-01-01`);
        params.append("to", `${year}-12-31`);
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
