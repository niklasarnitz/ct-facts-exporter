import axios, { AxiosInstance } from "axios";

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

class ChurchToolsClient {
  private axiosInstance: AxiosInstance;
  private isAuthenticated = false;

  constructor() {
    this.axiosInstance = axios.create({
      timeout: 30000,
      headers: {
        "Content-Type": "application/json",
      },
    });
  }

  async authenticate(baseUrl: string, loginToken: string): Promise<void> {
    this.axiosInstance.defaults.baseURL = `${baseUrl.replace(/\/$/, "")}/api`;
    this.axiosInstance.defaults.headers.common[
      "Authorization"
    ] = `Login ${loginToken}`;

    try {
      // Test authentication by calling whoami
      const response = await this.axiosInstance.get("/whoami");

      if (response.data && response.data.data && response.data.data.id) {
        this.isAuthenticated = true;
        console.log(
          "Successfully authenticated with ChurchTools using login token"
        );
        console.log(
          "User:",
          response.data.data.firstName,
          response.data.data.lastName
        );
      } else {
        throw new Error("Invalid authentication response");
      }
    } catch (error) {
      this.isAuthenticated = false;
      console.error("Authentication failed:", error);
      throw new Error(`Authentication failed: ${error}`);
    }
  }

  isAuth(): boolean {
    return this.isAuthenticated;
  }

  async getMasterData(): Promise<CTMasterData> {
    if (!this.isAuthenticated) {
      throw new Error("Not authenticated");
    }

    try {
      const response = await this.axiosInstance.get("/event/masterdata");
      return response.data.data || { facts: [] };
    } catch (error) {
      console.error("Error fetching master data:", error);
      throw error;
    }
  }

  async getEvents(
    yearOrFrom?: number | string,
    to?: string
  ): Promise<CTEvent[]> {
    if (!this.isAuthenticated) {
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
      const response = await this.axiosInstance.get(url);

      // Handle both wrapped and unwrapped responses
      if (response.data.data) {
        return response.data.data;
      }
      return response.data || [];
    } catch (error) {
      console.error("Error fetching events:", error);
      throw error;
    }
  }

  async getEventFacts(eventId: number): Promise<CTEventFact[]> {
    if (!this.isAuthenticated) {
      throw new Error("Not authenticated");
    }

    try {
      const response = await this.axiosInstance.get(`/events/${eventId}/facts`);
      return response.data.data || [];
    } catch (error) {
      console.error(`Error fetching facts for event ${eventId}:`, error);
      // Return empty array instead of throwing to continue with other events
      return [];
    }
  }

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
  }

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
  }

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
  }
}

// Export a singleton instance
export const ctClient = new ChurchToolsClient();
