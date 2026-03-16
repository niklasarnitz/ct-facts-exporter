import { Database } from "bun:sqlite";
import path from "path";

const db = new Database(path.join(process.cwd(), "data.db"));

// Create tables
db.exec(`
  CREATE TABLE IF NOT EXISTS facts (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    name_translated TEXT NOT NULL,
    type TEXT NOT NULL,
    unit TEXT,
    sort_key INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    start_date TEXT NOT NULL,
    end_date TEXT,
    calendar_id INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS event_facts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL,
    fact_id INTEGER NOT NULL,
    event_name TEXT,
    value REAL,
    value_text TEXT,
    modified_date TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (event_id) REFERENCES events(id),
    FOREIGN KEY (fact_id) REFERENCES facts(id),
    UNIQUE(event_id, fact_id)
  );

  CREATE TABLE IF NOT EXISTS church_groups (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    group_type_id INTEGER,
    group_status_id INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS group_roles (
    id INTEGER PRIMARY KEY,
    group_id INTEGER NOT NULL,
    group_type_role_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    name_translated TEXT,
    is_leader INTEGER,
    sort_key INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (group_id) REFERENCES church_groups(id),
    UNIQUE(group_id, group_type_role_id)
  );

  CREATE TABLE IF NOT EXISTS group_memberships (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id INTEGER NOT NULL,
    person_id INTEGER NOT NULL,
    person_name TEXT,
    group_member_status TEXT NOT NULL,
    group_type_role_id INTEGER NOT NULL,
    member_start_date TEXT,
    member_end_date TEXT,
    registered_by INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (group_id) REFERENCES church_groups(id),
    UNIQUE(group_id, person_id)
  );

  CREATE TABLE IF NOT EXISTS group_meetings (
    id INTEGER PRIMARY KEY,
    group_id INTEGER NOT NULL,
    start_date TEXT NOT NULL,
    end_date TEXT,
    is_canceled INTEGER,
    is_completed INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (group_id) REFERENCES church_groups(id)
  );

  CREATE TABLE IF NOT EXISTS group_meeting_attendances (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    meeting_id INTEGER NOT NULL,
    group_id INTEGER NOT NULL,
    person_id INTEGER NOT NULL,
    attendance_status TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (meeting_id) REFERENCES group_meetings(id),
    FOREIGN KEY (group_id) REFERENCES church_groups(id),
    UNIQUE(meeting_id, person_id)
  );

  CREATE INDEX IF NOT EXISTS idx_event_facts_event_id ON event_facts(event_id);
  CREATE INDEX IF NOT EXISTS idx_event_facts_fact_id ON event_facts(fact_id);
  CREATE INDEX IF NOT EXISTS idx_event_facts_event_name ON event_facts(event_name);
  CREATE INDEX IF NOT EXISTS idx_events_start_date ON events(start_date);
  CREATE INDEX IF NOT EXISTS idx_events_calendar_id ON events(calendar_id);
  CREATE INDEX IF NOT EXISTS idx_group_roles_group_id ON group_roles(group_id);
  CREATE INDEX IF NOT EXISTS idx_group_roles_type_id ON group_roles(group_type_role_id);
  CREATE INDEX IF NOT EXISTS idx_group_memberships_group_id ON group_memberships(group_id);
  CREATE INDEX IF NOT EXISTS idx_group_memberships_type_id ON group_memberships(group_type_role_id);
  CREATE INDEX IF NOT EXISTS idx_group_memberships_start_date ON group_memberships(member_start_date);
  CREATE INDEX IF NOT EXISTS idx_group_memberships_status ON group_memberships(group_member_status);
  CREATE INDEX IF NOT EXISTS idx_group_meetings_group_id ON group_meetings(group_id);
  CREATE INDEX IF NOT EXISTS idx_group_meetings_start_date ON group_meetings(start_date);
  CREATE INDEX IF NOT EXISTS idx_group_meeting_attendances_group_id ON group_meeting_attendances(group_id);
  CREATE INDEX IF NOT EXISTS idx_group_meeting_attendances_meeting_id ON group_meeting_attendances(meeting_id);
  CREATE INDEX IF NOT EXISTS idx_group_meeting_attendances_person_id ON group_meeting_attendances(person_id);
  CREATE INDEX IF NOT EXISTS idx_group_meeting_attendances_status ON group_meeting_attendances(attendance_status);
`);

// Backwards-compatible migration for existing databases created before person_name was added.
try {
  db.exec(`ALTER TABLE group_memberships ADD COLUMN person_name TEXT;`);
} catch {
  // Column already exists.
}

function normalizeFactEventName(eventName?: string): string | null {
  if (!eventName) {
    return null;
  }

  const trimmed = eventName.trim().replace(/\s+/g, " ");
  if (!trimmed) {
    return null;
  }

  // Normalize variants like "Gottesdienst mit ..." to "Gottesdienst"
  // and "Kontakt Gottesdienst mit ..." to "Kontakt Gottesdienst"
  // so Grafana filters can target one stable event category.
  if (/^kontakt\s+gottesdienst\s+mit\b/iu.test(trimmed)) {
    return "Kontakt Gottesdienst";
  }

  if (/^gottesdienst\s+mit\b/iu.test(trimmed)) {
    return "Gottesdienst";
  }

  return trimmed;
}

// Prepared statements
const stmts = {
  getFact: db.prepare(`
    SELECT * FROM facts WHERE id = ?
  `),

  insertFact: db.prepare(`
    INSERT OR REPLACE INTO facts (id, name, name_translated, type, unit, sort_key)
    VALUES (?, ?, ?, ?, ?, ?)
  `),

  updateFactName: db.prepare(`
    UPDATE facts SET name = ?, name_translated = ? WHERE id = ?
  `),

  insertEvent: db.prepare(`
    INSERT OR REPLACE INTO events (id, name, start_date, end_date, calendar_id)
    VALUES (?, ?, ?, ?, ?)
  `),

  insertEventFact: db.prepare(`
    INSERT OR REPLACE INTO event_facts (event_id, fact_id, event_name, value, value_text, modified_date)
    VALUES (?, ?, ?, ?, ?, ?)
  `),

  upsertGroup: db.prepare(`
    INSERT OR REPLACE INTO church_groups (id, name, group_type_id, group_status_id)
    VALUES (?, ?, ?, ?)
  `),

  deleteGroupRolesByGroup: db.prepare(`
    DELETE FROM group_roles WHERE group_id = ?
  `),

  upsertGroupRole: db.prepare(`
    INSERT OR REPLACE INTO group_roles (
      id,
      group_id,
      group_type_role_id,
      name,
      name_translated,
      is_leader,
      sort_key
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `),

  deleteGroupMembershipsByGroup: db.prepare(`
    DELETE FROM group_memberships WHERE group_id = ?
  `),

  upsertGroupMembership: db.prepare(`
    INSERT OR REPLACE INTO group_memberships (
      group_id,
      person_id,
      person_name,
      group_member_status,
      group_type_role_id,
      member_start_date,
      member_end_date,
      registered_by,
      updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
  `),

  deleteGroupMeetingAttendancesByGroup: db.prepare(`
    DELETE FROM group_meeting_attendances WHERE group_id = ?
  `),

  deleteGroupMeetingsByGroup: db.prepare(`
    DELETE FROM group_meetings WHERE group_id = ?
  `),

  upsertGroupMeeting: db.prepare(`
    INSERT OR REPLACE INTO group_meetings (
      id,
      group_id,
      start_date,
      end_date,
      is_canceled,
      is_completed,
      updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
  `),

  upsertGroupMeetingAttendance: db.prepare(`
    INSERT OR REPLACE INTO group_meeting_attendances (
      meeting_id,
      group_id,
      person_id,
      attendance_status,
      updated_at
    )
    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
  `),

  getNumericFacts: db.prepare(`
    SELECT DISTINCT f.id, f.name, f.name_translated, f.unit
    FROM facts f
    WHERE f.type = 'number'
    ORDER BY f.sort_key
  `),

  getEventNames: db.prepare(`
    SELECT DISTINCT event_name
    FROM event_facts
    WHERE event_name IS NOT NULL
    ORDER BY event_name
  `),

  getCalendarIds: db.prepare(`
    SELECT DISTINCT calendar_id
    FROM events
    WHERE calendar_id IS NOT NULL
    ORDER BY calendar_id
  `),

  getChurchGroups: db.prepare(`
    SELECT id, name, group_type_id, group_status_id
    FROM church_groups
    ORDER BY name
  `),

  getEventFactsByFactId: db.prepare(`
    SELECT 
      ef.event_id,
      ef.fact_id,
      ef.value,
      ef.event_name,
      e.name as event_name_full,
      e.start_date,
      e.end_date,
      f.name as fact_name,
      f.unit
    FROM event_facts ef
    JOIN events e ON ef.event_id = e.id
    JOIN facts f ON ef.fact_id = f.id
    WHERE ef.fact_id = ? AND ef.value IS NOT NULL
    ORDER BY e.start_date
  `),

  getEventFactsByTimeRange: db.prepare(`
    SELECT 
      ef.event_id,
      ef.fact_id,
      ef.value,
      ef.event_name,
      e.name as event_name_full,
      e.start_date,
      e.end_date,
      f.name as fact_name,
      f.unit
    FROM event_facts ef
    JOIN events e ON ef.event_id = e.id
    JOIN facts f ON ef.fact_id = f.id
    WHERE ef.fact_id = ? 
      AND ef.value IS NOT NULL
      AND datetime(e.start_date) >= datetime(?)
      AND datetime(e.start_date) <= datetime(?)
    ORDER BY e.start_date
  `),

  getLastSyncTime: db.prepare(`
    SELECT MAX(created_at) as last_sync FROM events
  `),
};

export interface Fact {
  id: number;
  name: string;
  name_translated: string;
  type: string;
  unit?: string;
  sort_key: number;
}

export interface Event {
  id: number;
  name: string;
  start_date: string;
  end_date?: string;
  calendar_id?: number;
}

export interface EventFact {
  event_id: number;
  fact_id: number;
  value?: number;
  value_text?: string;
  event_name?: string;
  event_name_full: string;
  start_date: string;
  end_date?: string;
  calendar_id?: number;
  fact_name: string;
  unit?: string;
}

export interface FactValueRow {
  event_id: number;
  fact_id: number;
  value: number;
  event_name?: string;
  event_name_full: string;
  start_date: string;
  end_date?: string;
  calendar_id?: number;
  fact_name: string;
  unit?: string;
}

export interface EventCoverageRow {
  event_id: number;
  start_date: string;
  calendar_id?: number;
  has_value: number;
}

export interface ChurchGroup {
  id: number;
  name: string;
  group_type_id?: number;
  group_status_id?: number;
}

export interface GroupRole {
  id: number;
  group_id: number;
  group_type_role_id: number;
  name: string;
  name_translated?: string;
  is_leader?: boolean;
  sort_key?: number;
}

export interface GroupMembership {
  group_id: number;
  person_id: number;
  person_name?: string;
  group_member_status: string;
  group_type_role_id: number;
  member_start_date?: string;
  member_end_date?: string;
  registered_by?: number;
}

export type GroupMeetingAttendanceStatus =
  | "absent"
  | "not-in-group"
  | "present"
  | "unsure";

export interface GroupMeeting {
  id: number;
  group_id: number;
  start_date: string;
  end_date?: string;
  is_canceled?: boolean;
  is_completed?: boolean;
}

export interface GroupMeetingAttendance {
  meeting_id: number;
  group_id: number;
  person_id: number;
  attendance_status: GroupMeetingAttendanceStatus;
}

export interface GroupMeetingAttendanceRow {
  meeting_id: number;
  group_id: number;
  group_name: string;
  person_id: number;
  person_name: string;
  attendance_status: GroupMeetingAttendanceStatus;
  start_date: string;
  is_canceled: number;
  is_completed: number;
}

export interface GroupMeetingPerson {
  person_id: number;
  person_name: string;
}

export interface GroupMemberType {
  group_type_role_id: number;
  name: string;
}

export interface GroupMembershipRow {
  group_id: number;
  group_name: string;
  person_id: number;
  group_member_status: string;
  group_type_role_id: number;
  group_member_type_name: string;
  member_start_date?: string;
  member_end_date?: string;
}

export const database = {
  upsertFact(fact: Fact) {
    // Check if fact exists
    const existing = stmts.getFact.get(fact.id) as Fact | undefined;

    if (existing) {
      // Fact exists, check if name changed
      if (
        existing.name !== fact.name ||
        existing.name_translated !== fact.name_translated
      ) {
        console.log(
          `Updating fact ${fact.id}: "${existing.name_translated}" -> "${fact.name_translated}"`
        );
        stmts.updateFactName.run(fact.name, fact.name_translated, fact.id);
      }
      // Note: We don't update other fields like type, unit, sort_key as they are structural
    } else {
      // Fact doesn't exist, insert it
      console.log(`Inserting new fact ${fact.id}: "${fact.name_translated}"`);
      stmts.insertFact.run(
        fact.id,
        fact.name,
        fact.name_translated,
        fact.type,
        fact.unit || null,
        fact.sort_key
      );
    }
  },

  insertFact(fact: Fact) {
    stmts.insertFact.run(
      fact.id,
      fact.name,
      fact.name_translated,
      fact.type,
      fact.unit || null,
      fact.sort_key
    );
  },

  insertEvent(event: Event) {
    stmts.insertEvent.run(
      event.id,
      event.name,
      event.start_date,
      event.end_date || null,
      event.calendar_id || null
    );
  },

  insertEventFact(eventFact: {
    event_id: number;
    fact_id: number;
    value?: number | string;
    modified_date?: string;
    event_name?: string;
  }) {
    const isNumeric = typeof eventFact.value === "number";
    const numericValue = isNumeric ? (eventFact.value as number) : null;
    const textValue =
      !isNumeric && eventFact.value !== undefined
        ? String(eventFact.value)
        : null;

    stmts.insertEventFact.run(
      eventFact.event_id,
      eventFact.fact_id,
      normalizeFactEventName(eventFact.event_name),
      numericValue,
      textValue,
      eventFact.modified_date || null
    );
  },

  getNumericFacts(): Fact[] {
    return stmts.getNumericFacts.all() as Fact[];
  },

  getEventNames(): string[] {
    const results = stmts.getEventNames.all() as { event_name: string }[];
    return results.map((r) => r.event_name);
  },

  getCalendarIds(): number[] {
    const results = stmts.getCalendarIds.all() as { calendar_id: number }[];
    return results.map((r) => r.calendar_id);
  },

  upsertGroup(group: ChurchGroup) {
    stmts.upsertGroup.run(
      group.id,
      group.name,
      group.group_type_id ?? null,
      group.group_status_id ?? null
    );
  },

  replaceGroupRoles(groupId: number, roles: GroupRole[]) {
    const tx = db.transaction((groupIdTx: number, rolesTx: GroupRole[]) => {
      stmts.deleteGroupRolesByGroup.run(groupIdTx);
      for (const role of rolesTx) {
        stmts.upsertGroupRole.run(
          role.id,
          role.group_id,
          role.group_type_role_id,
          role.name,
          role.name_translated ?? role.name,
          role.is_leader ? 1 : 0,
          role.sort_key ?? null
        );
      }
    });

    tx(groupId, roles);
  },

  replaceGroupMemberships(groupId: number, memberships: GroupMembership[]) {
    const tx = db.transaction(
      (groupIdTx: number, membershipsTx: GroupMembership[]) => {
        stmts.deleteGroupMembershipsByGroup.run(groupIdTx);
        for (const membership of membershipsTx) {
          stmts.upsertGroupMembership.run(
            membership.group_id,
            membership.person_id,
            membership.person_name ?? null,
            membership.group_member_status,
            membership.group_type_role_id,
            membership.member_start_date ?? null,
            membership.member_end_date ?? null,
            membership.registered_by ?? null
          );
        }
      }
    );

    tx(groupId, memberships);
  },

  replaceGroupMeetingsAndAttendances(
    groupId: number,
    meetings: GroupMeeting[],
    attendances: GroupMeetingAttendance[]
  ) {
    const tx = db.transaction(
      (
        groupIdTx: number,
        meetingsTx: GroupMeeting[],
        attendancesTx: GroupMeetingAttendance[]
      ) => {
        stmts.deleteGroupMeetingAttendancesByGroup.run(groupIdTx);
        stmts.deleteGroupMeetingsByGroup.run(groupIdTx);

        for (const meeting of meetingsTx) {
          stmts.upsertGroupMeeting.run(
            meeting.id,
            meeting.group_id,
            meeting.start_date,
            meeting.end_date ?? null,
            meeting.is_canceled ? 1 : 0,
            meeting.is_completed ? 1 : 0
          );
        }

        for (const attendance of attendancesTx) {
          stmts.upsertGroupMeetingAttendance.run(
            attendance.meeting_id,
            attendance.group_id,
            attendance.person_id,
            attendance.attendance_status
          );
        }
      }
    );

    tx(groupId, meetings, attendances);
  },

  getChurchGroups(): ChurchGroup[] {
    return stmts.getChurchGroups.all() as ChurchGroup[];
  },

  getGroupMemberTypes(groupIds?: number[]): GroupMemberType[] {
    const groupFilter =
      groupIds && groupIds.length > 0
        ? `WHERE gm.group_id IN (${groupIds.map(() => "?").join(",")})`
        : "";

    const sql = `
      SELECT DISTINCT
        gm.group_type_role_id,
        COALESCE(gr.name_translated, gr.name, 'Role ' || gm.group_type_role_id) as name
      FROM group_memberships gm
      LEFT JOIN group_roles gr
        ON gr.group_id = gm.group_id
       AND gr.group_type_role_id = gm.group_type_role_id
      ${groupFilter}
      ORDER BY name
    `;

    const stmt = db.prepare(sql);
    return groupIds && groupIds.length > 0
      ? (stmt.all(...groupIds) as GroupMemberType[])
      : (stmt.all() as GroupMemberType[]);
  },

  getGroupMeetingPeople(groupIds?: number[]): GroupMeetingPerson[] {
    const groupFilter =
      groupIds && groupIds.length > 0
        ? `WHERE gma.group_id IN (${groupIds.map(() => "?").join(",")})`
        : "";

    const sql = `
      SELECT
        gma.person_id,
        MIN(COALESCE(gm.person_name, 'Person ' || gma.person_id)) as person_name
      FROM group_meeting_attendances gma
      LEFT JOIN group_memberships gm
        ON gm.group_id = gma.group_id
       AND gm.person_id = gma.person_id
      ${groupFilter}
      GROUP BY gma.person_id
      ORDER BY person_name
    `;

    const stmt = db.prepare(sql);
    return groupIds && groupIds.length > 0
      ? (stmt.all(...groupIds) as GroupMeetingPerson[])
      : (stmt.all() as GroupMeetingPerson[]);
  },

  getGroupMembershipsByStartDateRange(
    from: string,
    to: string,
    groupIds?: number[],
    groupTypeRoleIds?: number[],
    groupMemberStatuses?: string[]
  ): GroupMembershipRow[] {
    const groupFilter =
      groupIds && groupIds.length > 0
        ? `AND gm.group_id IN (${groupIds.map(() => "?").join(",")})`
        : "";

    const roleFilter =
      groupTypeRoleIds && groupTypeRoleIds.length > 0
        ? `AND gm.group_type_role_id IN (${groupTypeRoleIds.map(() => "?").join(",")})`
        : "";

    const statusFilter =
      groupMemberStatuses && groupMemberStatuses.length > 0
        ? `AND gm.group_member_status IN (${groupMemberStatuses
            .map(() => "?")
            .join(",")})`
        : "";

    const sql = `
      SELECT
        gm.group_id,
        g.name as group_name,
        gm.person_id,
        gm.group_member_status,
        gm.group_type_role_id,
        COALESCE(gr.name_translated, gr.name, 'Role ' || gm.group_type_role_id) as group_member_type_name,
        gm.member_start_date,
        gm.member_end_date
      FROM group_memberships gm
      JOIN church_groups g ON g.id = gm.group_id
      LEFT JOIN group_roles gr
        ON gr.group_id = gm.group_id
       AND gr.group_type_role_id = gm.group_type_role_id
      WHERE gm.member_start_date IS NOT NULL
        AND datetime(gm.member_start_date) >= datetime(?)
        AND datetime(gm.member_start_date) <= datetime(?)
        ${groupFilter}
        ${roleFilter}
        ${statusFilter}
      ORDER BY gm.member_start_date
    `;

    const params: Array<number | string> = [from, to];

    if (groupIds && groupIds.length > 0) {
      params.push(...groupIds);
    }

    if (groupTypeRoleIds && groupTypeRoleIds.length > 0) {
      params.push(...groupTypeRoleIds);
    }

    if (groupMemberStatuses && groupMemberStatuses.length > 0) {
      params.push(...groupMemberStatuses);
    }

    const stmt = db.prepare(sql);
    return stmt.all(...params) as GroupMembershipRow[];
  },

  getGroupMeetingAttendancesByDateRange(
    from: string,
    to: string,
    groupIds?: number[],
    personIds?: number[],
    attendanceStatuses?: GroupMeetingAttendanceStatus[],
    includeCanceledMeetings: boolean = false,
    includeIncompleteMeetings: boolean = false
  ): GroupMeetingAttendanceRow[] {
    const groupFilter =
      groupIds && groupIds.length > 0
        ? `AND gma.group_id IN (${groupIds.map(() => "?").join(",")})`
        : "";

    const personFilter =
      personIds && personIds.length > 0
        ? `AND gma.person_id IN (${personIds.map(() => "?").join(",")})`
        : "";

    const statusFilter =
      attendanceStatuses && attendanceStatuses.length > 0
        ? `AND gma.attendance_status IN (${attendanceStatuses
            .map(() => "?")
            .join(",")})`
        : "";

    const canceledFilter = includeCanceledMeetings
      ? ""
      : "AND COALESCE(gm_meeting.is_canceled, 0) = 0";

    const completedFilter = includeIncompleteMeetings
      ? ""
      : "AND COALESCE(gm_meeting.is_completed, 0) = 1";

    const sql = `
      SELECT
        gma.meeting_id,
        gma.group_id,
        g.name as group_name,
        gma.person_id,
        COALESCE(gm.person_name, 'Person ' || gma.person_id) as person_name,
        gma.attendance_status,
        gm_meeting.start_date,
        COALESCE(gm_meeting.is_canceled, 0) as is_canceled,
        COALESCE(gm_meeting.is_completed, 0) as is_completed
      FROM group_meeting_attendances gma
      JOIN group_meetings gm_meeting ON gm_meeting.id = gma.meeting_id
      JOIN church_groups g ON g.id = gma.group_id
      LEFT JOIN group_memberships gm
        ON gm.group_id = gma.group_id
       AND gm.person_id = gma.person_id
      WHERE datetime(gm_meeting.start_date) >= datetime(?)
        AND datetime(gm_meeting.start_date) <= datetime(?)
        ${groupFilter}
        ${personFilter}
        ${statusFilter}
        ${canceledFilter}
        ${completedFilter}
      ORDER BY gm_meeting.start_date, person_name
    `;

    const params: Array<number | string> = [from, to];

    if (groupIds && groupIds.length > 0) {
      params.push(...groupIds);
    }

    if (personIds && personIds.length > 0) {
      params.push(...personIds);
    }

    if (attendanceStatuses && attendanceStatuses.length > 0) {
      params.push(...attendanceStatuses);
    }

    const stmt = db.prepare(sql);
    return stmt.all(...params) as GroupMeetingAttendanceRow[];
  },

  getEventFactsByFactId(factId: number): EventFact[] {
    return stmts.getEventFactsByFactId.all(factId) as EventFact[];
  },

  getEventFactsByTimeRange(
    factId: number,
    from: string,
    to: string
  ): EventFact[] {
    return stmts.getEventFactsByTimeRange.all(factId, from, to) as EventFact[];
  },

  getEventFactsByTimeRangeFiltered(
    factId: number,
    from: string,
    to: string,
    eventNames?: string[]
  ): EventFact[] {
    if (!eventNames || eventNames.length === 0) {
      return this.getEventFactsByTimeRange(factId, from, to);
    }

    const placeholders = eventNames.map(() => "?").join(",");
    const sql = `
      SELECT 
        ef.event_id,
        ef.fact_id,
        ef.value,
        ef.event_name,
        e.name as event_name_full,
        e.start_date,
        e.end_date,
        e.calendar_id,
        f.name as fact_name,
        f.unit
      FROM event_facts ef
      JOIN events e ON ef.event_id = e.id
      JOIN facts f ON ef.fact_id = f.id
      WHERE ef.fact_id = ? 
        AND ef.value IS NOT NULL
        AND datetime(e.start_date) >= datetime(?)
        AND datetime(e.start_date) <= datetime(?)
        AND ef.event_name IN (${placeholders})
      ORDER BY e.start_date
    `;

    const stmt = db.prepare(sql);
    return stmt.all(factId, from, to, ...eventNames) as EventFact[];
  },

  getFactValuesByTimeRange(
    factId: number,
    from: string,
    to: string,
    eventNames?: string[],
    calendarIds?: number[]
  ): FactValueRow[] {
    const eventNamesFilter =
      eventNames && eventNames.length > 0
        ? `AND ef.event_name IN (${eventNames.map(() => "?").join(",")})`
        : "";

    const calendarFilter =
      calendarIds && calendarIds.length > 0
        ? `AND e.calendar_id IN (${calendarIds.map(() => "?").join(",")})`
        : "";

    const sql = `
      SELECT
        ef.event_id,
        ef.fact_id,
        ef.value,
        ef.event_name,
        e.name as event_name_full,
        e.start_date,
        e.end_date,
        e.calendar_id,
        f.name as fact_name,
        f.unit
      FROM event_facts ef
      JOIN events e ON ef.event_id = e.id
      JOIN facts f ON ef.fact_id = f.id
      WHERE ef.fact_id = ?
        AND ef.value IS NOT NULL
        AND datetime(e.start_date) >= datetime(?)
        AND datetime(e.start_date) <= datetime(?)
        ${eventNamesFilter}
        ${calendarFilter}
      ORDER BY e.start_date
    `;

    const stmt = db.prepare(sql);
    const params: Array<number | string> = [factId, from, to];

    if (eventNames && eventNames.length > 0) {
      params.push(...eventNames);
    }

    if (calendarIds && calendarIds.length > 0) {
      params.push(...calendarIds);
    }

    return stmt.all(...params) as FactValueRow[];
  },

  getEventCoverageByTimeRange(
    factId: number,
    from: string,
    to: string,
    eventNames?: string[],
    calendarIds?: number[]
  ): EventCoverageRow[] {
    const calendarFilter =
      calendarIds && calendarIds.length > 0
        ? `AND e.calendar_id IN (${calendarIds.map(() => "?").join(",")})`
        : "";

    const eventNameFilter =
      eventNames && eventNames.length > 0
        ? `AND EXISTS (
            SELECT 1
            FROM event_facts ef_name
            WHERE ef_name.event_id = e.id
              AND ef_name.event_name IN (${eventNames.map(() => "?").join(",")})
          )`
        : "";

    const sql = `
      SELECT
        e.id as event_id,
        e.start_date,
        e.calendar_id,
        CASE WHEN ef.value IS NOT NULL THEN 1 ELSE 0 END as has_value
      FROM events e
      LEFT JOIN event_facts ef
        ON ef.event_id = e.id
       AND ef.fact_id = ?
      WHERE datetime(e.start_date) >= datetime(?)
        AND datetime(e.start_date) <= datetime(?)
        ${calendarFilter}
        ${eventNameFilter}
      ORDER BY e.start_date
    `;

    const stmt = db.prepare(sql);
    const params: Array<number | string> = [factId, from, to];

    if (calendarIds && calendarIds.length > 0) {
      params.push(...calendarIds);
    }

    if (eventNames && eventNames.length > 0) {
      params.push(...eventNames);
    }

    return stmt.all(...params) as EventCoverageRow[];
  },

  getMonthlyAggregateByFact(
    factId: number,
    from: string,
    to: string,
    eventNames?: string[]
  ): Array<{ month: string; value: number; count: number }> {
    const eventNamesFilter =
      eventNames && eventNames.length > 0
        ? `AND ef.event_name IN (${eventNames.map(() => "?").join(",")})`
        : "";

    const sql = `
      SELECT 
        strftime('%Y-%m', e.start_date) as month,
        SUM(ef.value) as value,
        COUNT(*) as count
      FROM event_facts ef
      JOIN events e ON ef.event_id = e.id
      WHERE ef.fact_id = ? 
        AND ef.value IS NOT NULL
        AND datetime(e.start_date) >= datetime(?)
        AND datetime(e.start_date) <= datetime(?)
        ${eventNamesFilter}
      GROUP BY strftime('%Y-%m', e.start_date)
      ORDER BY month
    `;

    const stmt = db.prepare(sql);
    const params =
      eventNames && eventNames.length > 0
        ? [factId, from, to, ...eventNames]
        : [factId, from, to];

    return stmt.all(...params) as Array<{
      month: string;
      value: number;
      count: number;
    }>;
  },

  getYearlySum(
    factId: number,
    year: number,
    eventNames?: string[]
  ): { value: number; count: number } {
    const from = `${year}-01-01`;
    const to = `${year}-12-31`;

    const eventNamesFilter =
      eventNames && eventNames.length > 0
        ? `AND ef.event_name IN (${eventNames.map(() => "?").join(",")})`
        : "";

    const sql = `
      SELECT 
        SUM(ef.value) as value,
        COUNT(*) as count
      FROM event_facts ef
      JOIN events e ON ef.event_id = e.id
      WHERE ef.fact_id = ? 
        AND ef.value IS NOT NULL
        AND datetime(e.start_date) >= datetime(?)
        AND datetime(e.start_date) <= datetime(?)
        ${eventNamesFilter}
    `;

    const stmt = db.prepare(sql);
    const params =
      eventNames && eventNames.length > 0
        ? [factId, from, to, ...eventNames]
        : [factId, from, to];

    const result = stmt.get(...params) as {
      value: number | null;
      count: number;
    };
    return { value: result.value || 0, count: result.count };
  },

  getYearlyMean(
    factId: number,
    year: number,
    eventNames?: string[]
  ): { value: number; count: number } {
    const from = `${year}-01-01`;
    const to = `${year}-12-31`;

    const eventNamesFilter =
      eventNames && eventNames.length > 0
        ? `AND ef.event_name IN (${eventNames.map(() => "?").join(",")})`
        : "";

    const sql = `
      SELECT 
        AVG(ef.value) as value,
        COUNT(*) as count
      FROM event_facts ef
      JOIN events e ON ef.event_id = e.id
      WHERE ef.fact_id = ? 
        AND ef.value IS NOT NULL
        AND datetime(e.start_date) >= datetime(?)
        AND datetime(e.start_date) <= datetime(?)
        ${eventNamesFilter}
    `;

    const stmt = db.prepare(sql);
    const params =
      eventNames && eventNames.length > 0
        ? [factId, from, to, ...eventNames]
        : [factId, from, to];

    const result = stmt.get(...params) as {
      value: number | null;
      count: number;
    };
    return { value: result.value || 0, count: result.count };
  },

  getLastSyncTime(): Date | null {
    const result = stmts.getLastSyncTime.get() as { last_sync: string | null };
    return result.last_sync ? new Date(result.last_sync) : null;
  },

  close() {
    db.close();
  },
};
