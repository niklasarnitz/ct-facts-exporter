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

export interface CTGroup {
  id: number;
  name: string;
  information?: {
    groupTypeId?: number;
    groupStatusId?: number;
  };
}

export interface CTGroupRole {
  id: number;
  groupTypeRoleId: number;
  name: string;
  nameTranslated?: string;
  isLeader?: boolean;
  sortKey?: number;
}

export interface CTGroupMember {
  personId?: number;
  person?: {
    id?: number;
    title?: string;
  };
  groupMemberStatus: "active" | "requested" | "waiting" | "to_delete";
  groupTypeRoleId: number;
  memberStartDate?: string | null;
  memberEndDate?: string | null;
  registeredBy?: number | null;
}

export type CTGroupMeetingAttendanceStatus =
  | "absent"
  | "not-in-group"
  | "present"
  | "unsure";

export interface CTGroupMeeting {
  id: number;
  groupId: number;
  startDate: string;
  endDate: string;
  isCanceled?: boolean;
  isCompleted?: boolean;
  attendances?: Record<string, CTGroupMeetingAttendanceStatus>;
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

  private shouldRetryRequest(error: unknown): boolean {
    if (!axios.isAxiosError(error)) {
      return false;
    }

    const status = error.response?.status;
    if (!status) {
      return true;
    }

    return status === 429 || status >= 500;
  }

  private async withRetry<T>(
    operationName: string,
    run: () => Promise<T>,
    maxRetries: number = 2,
    baseDelayMs: number = 400
  ): Promise<T> {
    let attempt = 0;

    while (true) {
      try {
        return await run();
      } catch (error) {
        if (attempt >= maxRetries || !this.shouldRetryRequest(error)) {
          throw error;
        }

        const retryDelayMs = baseDelayMs * 2 ** attempt;
        console.warn(
          `${operationName} failed (attempt ${attempt + 1}/${maxRetries + 1}). Retrying in ${retryDelayMs}ms...`
        );
        await new Promise((resolve) => setTimeout(resolve, retryDelayMs));
        attempt += 1;
      }
    }
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

  async getGroups(
    page: number = 1,
    limit: number = 200
  ): Promise<{
    groups: CTGroup[];
    lastPage: number;
  }> {
    if (!this.isAuthenticated) {
      throw new Error("Not authenticated");
    }

    const response = await this.withRetry("Fetch groups", () =>
      this.axiosInstance.get("/groups", {
        params: {
          page,
          limit,
        },
      })
    );

    const groups = (response.data?.data || []) as CTGroup[];
    const parsedLastPage = Number(response.data?.meta?.pagination?.lastPage);
    const lastPage =
      Number.isFinite(parsedLastPage) && parsedLastPage > 0
        ? Math.floor(parsedLastPage)
        : 1;

    return { groups, lastPage };
  }

  async getAllGroups(limit: number = 200): Promise<CTGroup[]> {
    const maxPages = 2000;
    const allGroups: CTGroup[] = [];
    let page = 1;
    let lastPage = 1;

    do {
      const response = await this.getGroups(page, limit);
      allGroups.push(...response.groups);
      lastPage = response.lastPage;
      page += 1;

      if (page > maxPages) {
        throw new Error(
          `Group pagination exceeded ${maxPages} pages. Aborting to avoid endless sync loop.`
        );
      }
    } while (page <= lastPage);

    return allGroups;
  }

  async getGroupRoles(groupId: number): Promise<CTGroupRole[]> {
    if (!this.isAuthenticated) {
      throw new Error("Not authenticated");
    }

    const response = await this.withRetry(`Fetch group roles (${groupId})`, () =>
      this.axiosInstance.get(`/groups/${groupId}/roles`)
    );
    return (response.data?.data || []) as CTGroupRole[];
  }

  async getGroupMembers(
    groupId: number,
    page: number = 1,
    limit: number = 200
  ): Promise<{
    members: CTGroupMember[];
    lastPage: number;
  }> {
    if (!this.isAuthenticated) {
      throw new Error("Not authenticated");
    }

    const response = await this.withRetry(
      `Fetch group members (${groupId}, page ${page})`,
      () =>
        this.axiosInstance.get(`/groups/${groupId}/members`, {
          params: {
            page,
            limit,
          },
        })
    );

    const members = (response.data?.data || []) as CTGroupMember[];
    const parsedLastPage = Number(response.data?.meta?.pagination?.lastPage);
    const lastPage =
      Number.isFinite(parsedLastPage) && parsedLastPage > 0
        ? Math.floor(parsedLastPage)
        : 1;

    return { members, lastPage };
  }

  async getAllGroupMembers(
    groupId: number,
    limit: number = 200
  ): Promise<CTGroupMember[]> {
    const maxPages = 2000;
    const allMembers: CTGroupMember[] = [];
    let page = 1;
    let lastPage = 1;

    do {
      const response = await this.getGroupMembers(groupId, page, limit);
      allMembers.push(...response.members);
      lastPage = response.lastPage;
      page += 1;

      if (page > maxPages) {
        throw new Error(
          `Member pagination exceeded ${maxPages} pages for group ${groupId}. Aborting to avoid endless sync loop.`
        );
      }
    } while (page <= lastPage);

    return allMembers;
  }

  async getGroupMeetings(
    groupId: number,
    page: number = 1,
    limit: number = 200,
    from?: string,
    to?: string
  ): Promise<{
    meetings: CTGroupMeeting[];
    lastPage: number;
  }> {
    if (!this.isAuthenticated) {
      throw new Error("Not authenticated");
    }

    const response = await this.withRetry(
      `Fetch group meetings (${groupId}, page ${page})`,
      () =>
        this.axiosInstance.get(`/groups/${groupId}/meetings`, {
          params: {
            page,
            limit,
            from,
            to,
            include: "attendances",
          },
        })
    );

    const meetings = (response.data?.data || []) as CTGroupMeeting[];
    const parsedLastPage = Number(response.data?.meta?.pagination?.lastPage);
    const lastPage =
      Number.isFinite(parsedLastPage) && parsedLastPage > 0
        ? Math.floor(parsedLastPage)
        : 1;

    return { meetings, lastPage };
  }

  async getAllGroupMeetings(
    groupId: number,
    from?: string,
    to?: string,
    limit: number = 200
  ): Promise<CTGroupMeeting[]> {
    const maxPages = 2000;
    const allMeetings: CTGroupMeeting[] = [];
    let page = 1;
    let lastPage = 1;

    do {
      const response = await this.getGroupMeetings(groupId, page, limit, from, to);
      allMeetings.push(...response.meetings);
      lastPage = response.lastPage;
      page += 1;

      if (page > maxPages) {
        throw new Error(
          `Meeting pagination exceeded ${maxPages} pages for group ${groupId}. Aborting to avoid endless sync loop.`
        );
      }
    } while (page <= lastPage);

    return allMeetings;
  }

  async getAllGroupMembershipData(
    meetingFrom?: string,
    meetingTo?: string
  ): Promise<{
    groups: CTGroup[];
    rolesByGroup: Map<number, CTGroupRole[]>;
    membersByGroup: Map<number, CTGroupMember[]>;
    meetingsByGroup: Map<number, CTGroupMeeting[]>;
    failedGroupIds: number[];
  }> {
    const groups = await this.getAllGroups(200);
    const rolesByGroup = new Map<number, CTGroupRole[]>();
    const membersByGroup = new Map<number, CTGroupMember[]>();
    const meetingsByGroup = new Map<number, CTGroupMeeting[]>();
    const failedGroupIds: number[] = [];

    console.log(`Fetching roles, memberships, and meetings for ${groups.length} groups...`);

    const batchSize = 5;
    for (let i = 0; i < groups.length; i += batchSize) {
      const batch = groups.slice(i, i + batchSize);
      const batchResults = await Promise.all(
        batch.map(async (group) => {
          try {
            const [roles, members, meetings] = await Promise.all([
              this.getGroupRoles(group.id),
              this.getAllGroupMembers(group.id, 200),
              this.getAllGroupMeetings(group.id, meetingFrom, meetingTo, 200),
            ]);

            return {
              groupId: group.id,
              roles,
              members,
              meetings,
            };
          } catch (error) {
            console.error(
              `Failed to sync group ${group.id} (${group.name}):`,
              error
            );
            failedGroupIds.push(group.id);
            return null;
          }
        })
      );

      for (const result of batchResults) {
        if (!result) {
          continue;
        }

        rolesByGroup.set(result.groupId, result.roles);
        membersByGroup.set(result.groupId, result.members);
        meetingsByGroup.set(result.groupId, result.meetings);
      }

      if ((i + batchSize) % 20 === 0 || i + batchSize >= groups.length) {
        console.log(
          `  Synced group metadata for ${Math.min(i + batchSize, groups.length)}/${groups.length} groups`
        );
      }
    }

    return {
      groups,
      rolesByGroup,
      membersByGroup,
      meetingsByGroup,
      failedGroupIds,
    };
  }
}

// Export a singleton instance
export const ctClient = new ChurchToolsClient();
