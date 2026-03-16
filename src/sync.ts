import cron, { ScheduledTask } from "node-cron";
import { ctClient } from "./churchtools-client";
import { database } from "./db";

export class DataSyncService {
  private syncTask: ScheduledTask | null = null;
  private isSyncing = false;

  private async syncGroupMemberships(meetingFrom?: string, meetingTo?: string) {
    console.log("Fetching group membership data...");
    const { groups, rolesByGroup, membersByGroup, meetingsByGroup, failedGroupIds } =
      await ctClient.getAllGroupMembershipData(meetingFrom, meetingTo);

    console.log(`Syncing ${groups.length} groups...`);

    const failedGroupIdSet = new Set(failedGroupIds);
    let syncedGroups = 0;
    let syncedMemberships = 0;
    let syncedMeetings = 0;
    let syncedAttendanceRows = 0;
    let skippedMembershipsWithoutPersonId = 0;
    let skippedAttendancesWithoutPersonId = 0;

    for (const group of groups) {
      database.upsertGroup({
        id: group.id,
        name: group.name,
        group_type_id: group.information?.groupTypeId,
        group_status_id: group.information?.groupStatusId,
      });

      if (failedGroupIdSet.has(group.id)) {
        continue;
      }

      const roles = rolesByGroup.get(group.id) || [];
      database.replaceGroupRoles(
        group.id,
        roles.map((role) => ({
          id: role.id,
          group_id: group.id,
          group_type_role_id: role.groupTypeRoleId,
          name: role.name,
          name_translated: role.nameTranslated,
          is_leader: role.isLeader,
          sort_key: role.sortKey,
        }))
      );

      const members = membersByGroup.get(group.id) || [];
      const normalizedMemberships = members
        .map((member) => {
          const personId = member.person?.id || member.personId;
          if (!personId) {
            skippedMembershipsWithoutPersonId += 1;
            return null;
          }

          return {
            group_id: group.id,
            person_id: personId,
            person_name: member.person?.title || undefined,
            group_member_status: member.groupMemberStatus,
            group_type_role_id: member.groupTypeRoleId,
            member_start_date: member.memberStartDate || undefined,
            member_end_date: member.memberEndDate || undefined,
            registered_by: member.registeredBy || undefined,
          };
        })
        .filter((member): member is NonNullable<typeof member> => member !== null);

      database.replaceGroupMemberships(
        group.id,
        normalizedMemberships
      );

      const meetings = meetingsByGroup.get(group.id) || [];
      const normalizedMeetings = meetings
        .map((meeting) => {
          if (!meeting.id || !meeting.startDate) {
            return null;
          }

          return {
            id: meeting.id,
            group_id: group.id,
            start_date: meeting.startDate,
            end_date: meeting.endDate || undefined,
            is_canceled: meeting.isCanceled,
            is_completed: meeting.isCompleted,
          };
        })
        .filter((meeting): meeting is NonNullable<typeof meeting> => meeting !== null);

      const knownMeetingIds = new Set(normalizedMeetings.map((meeting) => meeting.id));
      const normalizedAttendances = meetings.flatMap((meeting) => {
        if (!meeting.id || !knownMeetingIds.has(meeting.id)) {
          return [];
        }

        return Object.entries(meeting.attendances || {}).flatMap(
          ([personIdValue, attendanceStatus]) => {
            const personId = Number(personIdValue);
            if (!Number.isFinite(personId)) {
              skippedAttendancesWithoutPersonId += 1;
              return [];
            }

            return [
              {
                meeting_id: meeting.id,
                group_id: group.id,
                person_id: personId,
                attendance_status: attendanceStatus,
              },
            ];
          }
        );
      });

      database.replaceGroupMeetingsAndAttendances(
        group.id,
        normalizedMeetings,
        normalizedAttendances
      );

      syncedGroups += 1;
      syncedMemberships += normalizedMemberships.length;
      syncedMeetings += normalizedMeetings.length;
      syncedAttendanceRows += normalizedAttendances.length;
    }

    if (failedGroupIds.length > 0) {
      console.warn(
        `Skipped membership updates for ${failedGroupIds.length} groups due to API errors`
      );
    }

    if (skippedMembershipsWithoutPersonId > 0) {
      console.warn(
        `Skipped ${skippedMembershipsWithoutPersonId} memberships without person identifiers`
      );
    }

    if (skippedAttendancesWithoutPersonId > 0) {
      console.warn(
        `Skipped ${skippedAttendancesWithoutPersonId} attendance rows with invalid person identifiers`
      );
    }

    console.log(
      `Group data sync complete: ${syncedGroups} groups, ${syncedMemberships} memberships, ${syncedMeetings} meetings, ${syncedAttendanceRows} attendance rows`
    );
  }

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
      const groupMeetingFrom = `${now.getFullYear() - 1}-01-01`;
      const groupMeetingTo = `${now.getFullYear() + 1}-12-31`;

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

      await this.syncGroupMemberships(groupMeetingFrom, groupMeetingTo);

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

      await this.syncGroupMemberships(`${year}-01-01`, `${year}-12-31`);

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
