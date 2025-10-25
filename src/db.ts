import Database from "better-sqlite3";
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

  CREATE INDEX IF NOT EXISTS idx_event_facts_event_id ON event_facts(event_id);
  CREATE INDEX IF NOT EXISTS idx_event_facts_fact_id ON event_facts(fact_id);
  CREATE INDEX IF NOT EXISTS idx_event_facts_event_name ON event_facts(event_name);
  CREATE INDEX IF NOT EXISTS idx_events_start_date ON events(start_date);
`);

// Prepared statements
const stmts = {
  insertFact: db.prepare(`
    INSERT OR REPLACE INTO facts (id, name, name_translated, type, unit, sort_key)
    VALUES (?, ?, ?, ?, ?, ?)
  `),

  insertEvent: db.prepare(`
    INSERT OR REPLACE INTO events (id, name, start_date, end_date, calendar_id)
    VALUES (?, ?, ?, ?, ?)
  `),

  insertEventFact: db.prepare(`
    INSERT OR REPLACE INTO event_facts (event_id, fact_id, event_name, value, value_text, modified_date)
    VALUES (?, ?, ?, ?, ?, ?)
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
  fact_name: string;
  unit?: string;
}

export const database = {
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
    stmts.insertEventFact.run(
      eventFact.event_id,
      eventFact.fact_id,
      eventFact.event_name || null,
      isNumeric ? eventFact.value : null,
      !isNumeric ? String(eventFact.value) : null,
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
