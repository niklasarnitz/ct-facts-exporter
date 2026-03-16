import { Context } from "hono";
import {
  database,
  Fact,
  FactValueRow,
  EventCoverageRow,
  GroupMembershipRow,
  GroupMeetingAttendanceRow,
  GroupMeetingAttendanceStatus,
} from "./db";

const ALL_VALUE_TOKENS = new Set(["__all", "$__all", "all", "All", "*"]);

const FACT_AGGREGATION_DEFINITIONS = [
  { key: "raw", label: "Raw Data" },
  { key: "monthly", label: "Monthly Sum" },
  { key: "monthly_ratio", label: "Monthly Ratio (Numerator/Denominator)", includeDenominator: true },
  { key: "monthly_count", label: "Monthly Event Count" },
  { key: "monthly_rolling_3", label: "Monthly Rolling Average (3M)" },
  { key: "monthly_rolling_6", label: "Monthly Rolling Average (6M)" },
  { key: "ytd_cumulative", label: "YTD Cumulative" },
  { key: "monthly_distribution", label: "Monthly Distribution (Min/Median/P90/Max)" },
  { key: "monthly_anomaly_band", label: "Monthly Anomaly Band" },
  { key: "monthly_by_event", label: "Monthly By Event" },
  { key: "monthly_by_calendar", label: "Monthly By Calendar" },
  { key: "monthly_completeness", label: "Monthly Completeness (%)" },
  { key: "yearly_sum", label: "Yearly Sum" },
  { key: "yearly_mean", label: "Yearly Mean" },
  { key: "yearly_count", label: "Yearly Event Count" },
  { key: "yearly_distribution", label: "Yearly Distribution (Min/Median/P90/Max)" },
  { key: "yearly_yoy_delta", label: "Yearly YoY Delta" },
  { key: "yearly_yoy_percent", label: "Yearly YoY Percent" },
  { key: "yearly_completeness", label: "Yearly Completeness (%)" },
  { key: "top_bottom_table", label: "Top/Bottom Events Table", includeTopN: true },
] as const;

const FACT_AGGREGATION_KEYS = new Set(
  FACT_AGGREGATION_DEFINITIONS.map((d) => d.key)
);

const GROUP_MEMBERSHIP_AGGREGATION_DEFINITIONS = [
  { key: "monthly_by_type", label: "Membership Starts - Monthly By Type" },
  { key: "yearly_by_type", label: "Membership Starts - Yearly By Type" },
  { key: "yearly_total", label: "Membership Starts - Yearly Total" },
  {
    key: "yearly_yoy_by_type",
    label: "Membership Starts - Yearly YoY Delta By Type",
  },
  { key: "compare_table", label: "Membership Starts - Comparison Table" },
] as const;

const GROUP_MEMBERSHIP_AGGREGATION_KEYS = new Set(
  GROUP_MEMBERSHIP_AGGREGATION_DEFINITIONS.map((definition) => definition.key)
);

const GROUP_MEMBER_STATUSES = [
  "active",
  "requested",
  "waiting",
  "to_delete",
] as const;

const GROUP_MEMBERSHIP_TARGET_PREFIX = "group_memberships_";

const GROUP_MEETING_ATTENDANCE_STATUSES = [
  "absent",
  "not-in-group",
  "present",
  "unsure",
] as const;

const DEFAULT_GROUP_MEETING_ATTENDANCE_STATUSES: GroupMeetingAttendanceStatus[] = [
  "present",
  "absent",
  "unsure",
];

const GROUP_MEETING_ATTENDANCE_AGGREGATION_DEFINITIONS = [
  {
    key: "monthly_present_by_person",
    label: "Group Meetings - Monthly Present Count By Person",
  },
  {
    key: "monthly_rate_by_person",
    label: "Group Meetings - Monthly Attendance Rate By Person",
  },
  {
    key: "yearly_rate_by_person",
    label: "Group Meetings - Yearly Attendance Rate By Person",
  },
  {
    key: "person_summary_table",
    label: "Group Meetings - Person Attendance Summary Table",
  },
] as const;

const GROUP_MEETING_ATTENDANCE_AGGREGATION_KEYS = new Set(
  GROUP_MEETING_ATTENDANCE_AGGREGATION_DEFINITIONS.map((definition) => definition.key)
);

const GROUP_MEETING_ATTENDANCE_TARGET_PREFIX = "group_meeting_attendance_";

export interface GrafanaQueryRequest {
  targets: Array<{
    target: string;
    refId: string;
    type?: string;
    payload?: {
      event_names?: string[] | string;
      calendar_ids?: number[] | string[] | number | string;
      denominator_fact_id?: number | string;
      top_n?: number | string;
      group_ids?: number[] | string[] | number | string;
      group_type_role_ids?: number[] | string[] | number | string;
      group_member_statuses?: string[] | string;
      group_meeting_person_ids?: number[] | string[] | number | string;
      group_meeting_attendance_statuses?: string[] | string;
      include_canceled_meetings?: boolean | string | number;
      include_incomplete_meetings?: boolean | string | number;
      [key: string]: unknown;
    };
  }>;
  range: {
    from: string;
    to: string;
  };
  intervalMs?: number;
  maxDataPoints?: number;
}

export interface GrafanaDataPoint {
  target: string;
  datapoints: Array<[number, number]>;
  unit?: string;
}

export interface GrafanaTableResponse {
  type: "table";
  columns: Array<{ text: string; type: "string" | "number" | "time" }>;
  rows: Array<Array<string | number | null>>;
}

export interface GrafanaMetricOption {
  label: string;
  value: string;
  payloads?: Array<{
    name: string;
    label?: string;
    type: "select" | "multi-select" | "input";
    placeholder?: string;
    options?: Array<{ label: string; value: string | number }>;
  }>;
}

function parseJson(input: string): unknown {
  try {
    return JSON.parse(input);
  } catch {
    return undefined;
  }
}

function normalizeStringList(value: unknown): string[] | undefined {
  if (value === null || value === undefined) {
    return undefined;
  }

  if (Array.isArray(value)) {
    const normalized = value
      .flatMap((entry) => normalizeStringList(entry) || [])
      .map((entry) => entry.trim())
      .filter((entry) => entry.length > 0)
      .filter((entry) => !ALL_VALUE_TOKENS.has(entry));
    return normalized.length > 0 ? normalized : undefined;
  }

  if (typeof value !== "string") {
    return undefined;
  }

  const trimmed = value.trim();
  if (!trimmed || ALL_VALUE_TOKENS.has(trimmed)) {
    return undefined;
  }

  const parsed = parseJson(trimmed);
  if (parsed !== undefined) {
    return normalizeStringList(parsed);
  }

  if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
    const values = trimmed.slice(1, -1).split(",");
    return normalizeStringList(values);
  }

  if (trimmed.includes(",")) {
    return normalizeStringList(trimmed.split(","));
  }

  return [trimmed];
}

function normalizeNumberList(value: unknown): number[] | undefined {
  if (value === null || value === undefined) {
    return undefined;
  }

  if (Array.isArray(value)) {
    const numbers = value
      .flatMap((entry) => normalizeNumberList(entry) || [])
      .filter((entry) => Number.isFinite(entry));
    return numbers.length > 0 ? numbers : undefined;
  }

  if (typeof value === "number") {
    return Number.isFinite(value) ? [value] : undefined;
  }

  if (typeof value !== "string") {
    return undefined;
  }

  const trimmed = value.trim();
  if (!trimmed || ALL_VALUE_TOKENS.has(trimmed)) {
    return undefined;
  }

  const parsed = parseJson(trimmed);
  if (parsed !== undefined) {
    return normalizeNumberList(parsed);
  }

  if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
    const values = trimmed.slice(1, -1).split(",");
    return normalizeNumberList(values);
  }

  if (trimmed.includes(",")) {
    return normalizeNumberList(trimmed.split(","));
  }

  const parsedNumber = Number(trimmed);
  return Number.isFinite(parsedNumber) ? [parsedNumber] : undefined;
}

function normalizePositiveInteger(value: unknown, fallback: number): number {
  const parsed = normalizeNumberList(value);
  const first = parsed?.[0];
  if (!first || first <= 0) {
    return fallback;
  }
  return Math.round(first);
}

function normalizeBoolean(value: unknown, fallback: boolean): boolean {
  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "number") {
    if (value === 1) {
      return true;
    }
    if (value === 0) {
      return false;
    }
    return fallback;
  }

  if (typeof value !== "string") {
    return fallback;
  }

  const trimmed = value.trim().toLowerCase();
  if (trimmed === "true" || trimmed === "1" || trimmed === "yes") {
    return true;
  }
  if (trimmed === "false" || trimmed === "0" || trimmed === "no") {
    return false;
  }

  const parsed = parseJson(trimmed);
  if (typeof parsed === "boolean") {
    return parsed;
  }

  return fallback;
}

function normalizeMeetingAttendanceStatuses(
  value: unknown
): GroupMeetingAttendanceStatus[] | undefined {
  const statuses = normalizeStringList(value);
  if (!statuses || statuses.length === 0) {
    return undefined;
  }

  const allowedStatuses = new Set<GroupMeetingAttendanceStatus>(
    GROUP_MEETING_ATTENDANCE_STATUSES
  );
  const normalizedStatuses = statuses.filter((status): status is GroupMeetingAttendanceStatus =>
    allowedStatuses.has(status as GroupMeetingAttendanceStatus)
  );

  return normalizedStatuses.length > 0 ? normalizedStatuses : undefined;
}

function monthKeyFromDate(dateValue: string): string {
  return dateValue.slice(0, 7);
}

function yearFromDate(dateValue: string): number {
  return new Date(dateValue).getUTCFullYear();
}

function monthTimestamp(monthKey: string): number {
  const [year, month] = monthKey.split("-").map(Number);
  return Date.UTC(year, month - 1, 1);
}

function yearTimestamp(year: number): number {
  return Date.UTC(year, 0, 1);
}

function buildMonthRange(from: string, to: string): string[] {
  const startDate = new Date(from);
  const endDate = new Date(to);

  const cursor = new Date(Date.UTC(startDate.getUTCFullYear(), startDate.getUTCMonth(), 1));
  const end = new Date(Date.UTC(endDate.getUTCFullYear(), endDate.getUTCMonth(), 1));

  const months: string[] = [];
  while (cursor <= end) {
    const year = cursor.getUTCFullYear();
    const month = String(cursor.getUTCMonth() + 1).padStart(2, "0");
    months.push(`${year}-${month}`);
    cursor.setUTCMonth(cursor.getUTCMonth() + 1);
  }

  return months;
}

function toSeries(
  target: string,
  datapoints: Array<[number, number]>,
  unit?: string
): GrafanaDataPoint {
  return unit ? { target, datapoints, unit } : { target, datapoints };
}

function average(values: number[]): number {
  if (values.length === 0) {
    return 0;
  }
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function percentile(values: number[], percentileValue: number): number {
  if (values.length === 0) {
    return 0;
  }

  const sorted = [...values].sort((a, b) => a - b);
  const index = (percentileValue / 100) * (sorted.length - 1);
  const lowerIndex = Math.floor(index);
  const upperIndex = Math.ceil(index);

  if (lowerIndex === upperIndex) {
    return sorted[lowerIndex];
  }

  const ratio = index - lowerIndex;
  return sorted[lowerIndex] + (sorted[upperIndex] - sorted[lowerIndex]) * ratio;
}

function buildFilterLabel(eventNames?: string[], calendarIds?: number[]): string {
  const fragments: string[] = [];

  if (eventNames && eventNames.length > 0) {
    fragments.push(`events: ${eventNames.join(", ")}`);
  }

  if (calendarIds && calendarIds.length > 0) {
    fragments.push(`calendars: ${calendarIds.join(", ")}`);
  }

  return fragments.length > 0 ? ` [${fragments.join(" | ")}]` : "";
}

function buildMetricOptions(): GrafanaMetricOption[] {
  const facts = database.getNumericFacts();
  const eventNames = database.getEventNames();
  const calendarIds = database.getCalendarIds();
  const groups = database.getChurchGroups();
  const groupMemberTypes = database.getGroupMemberTypes();
  const groupMeetingPeople = database.getGroupMeetingPeople();

  const factMetrics = facts.flatMap((fact) => {
    const factLabel = fact.unit
      ? `${fact.name_translated} (${fact.unit})`
      : fact.name_translated;

    return FACT_AGGREGATION_DEFINITIONS.map((aggregation) => {
      const payloads: NonNullable<GrafanaMetricOption["payloads"]> = [
        {
          name: "event_names",
          label: "Filter By Event Names",
          type: "multi-select",
          placeholder: "Optional event-name filter",
          options: eventNames.map((name) => ({ label: name, value: name })),
        },
        {
          name: "calendar_ids",
          label: "Filter By Calendar IDs",
          type: "multi-select",
          placeholder: "Optional calendar filter",
          options: calendarIds.map((id) => ({
            label: `Calendar ${id}`,
            value: id,
          })),
        },
      ];

      if (aggregation.includeTopN) {
        payloads.push({
          name: "top_n",
          label: "Top/Bottom Rows",
          type: "input",
          placeholder: "Number of rows per direction (default: 5)",
        });
      }

      if (aggregation.includeDenominator) {
        payloads.push({
          name: "denominator_fact_id",
          label: "Denominator Fact ID",
          type: "input",
          placeholder: "Fact ID for denominator",
        });
      }

      return {
        label: `${factLabel} - ${aggregation.label}`,
        value: `fact_${fact.id}_${aggregation.key}`,
        payloads,
      };
    });
  });

  const groupMetrics = GROUP_MEMBERSHIP_AGGREGATION_DEFINITIONS.map(
    (aggregation) => {
      const payloads: NonNullable<GrafanaMetricOption["payloads"]> = [
        {
          name: "group_ids",
          label: "Filter By Groups",
          type: "multi-select",
          placeholder: "Select groups (e.g. Freizeiten)",
          options: groups.map((group) => ({
            label: group.name,
            value: group.id,
          })),
        },
        {
          name: "group_type_role_ids",
          label: "Filter By Group Member Types",
          type: "multi-select",
          placeholder: "Optional role/member-type filter",
          options: groupMemberTypes.map((type) => ({
            label: type.name,
            value: type.group_type_role_id,
          })),
        },
        {
          name: "group_member_statuses",
          label: "Filter By Membership Status",
          type: "multi-select",
          placeholder: "Optional membership status filter",
          options: GROUP_MEMBER_STATUSES.map((status) => ({
            label: status,
            value: status,
          })),
        },
      ];

      return {
        label: aggregation.label,
        value: `${GROUP_MEMBERSHIP_TARGET_PREFIX}${aggregation.key}`,
        payloads,
      };
    }
  );

  const groupMeetingMetrics = GROUP_MEETING_ATTENDANCE_AGGREGATION_DEFINITIONS.map(
    (aggregation) => {
      const payloads: NonNullable<GrafanaMetricOption["payloads"]> = [
        {
          name: "group_ids",
          label: "Filter By Groups",
          type: "multi-select",
          placeholder: "Optional group filter",
          options: groups.map((group) => ({
            label: group.name,
            value: group.id,
          })),
        },
        {
          name: "group_meeting_person_ids",
          label: "Filter By People",
          type: "multi-select",
          placeholder: "Optional person filter",
          options: groupMeetingPeople.map((person) => ({
            label: person.person_name,
            value: person.person_id,
          })),
        },
        {
          name: "group_meeting_attendance_statuses",
          label: "Attendance Statuses",
          type: "multi-select",
          placeholder: "Defaults to present/absent/unsure",
          options: GROUP_MEETING_ATTENDANCE_STATUSES.map((status) => ({
            label: status,
            value: status,
          })),
        },
        {
          name: "include_canceled_meetings",
          label: "Include Canceled Meetings",
          type: "input",
          placeholder: "false (default) or true",
        },
        {
          name: "include_incomplete_meetings",
          label: "Include Incomplete Meetings",
          type: "input",
          placeholder: "false (default) or true",
        },
      ];

      return {
        label: aggregation.label,
        value: `${GROUP_MEETING_ATTENDANCE_TARGET_PREFIX}${aggregation.key}`,
        payloads,
      };
    }
  );

  return [...factMetrics, ...groupMetrics, ...groupMeetingMetrics];
}

function normalizeVariablePayload(body: unknown): Record<string, unknown> {
  if (typeof body === "string") {
    const parsed = parseJson(body);
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      return normalizeVariablePayload(parsed);
    }
    return { target: body };
  }

  if (!body || typeof body !== "object" || Array.isArray(body)) {
    return {};
  }

  const record = body as Record<string, unknown>;
  const topLevelTarget =
    typeof record.target === "string" && record.target.trim().length > 0
      ? record.target
      : undefined;

  const withTarget = (payload: Record<string, unknown>): Record<string, unknown> => {
    if (!topLevelTarget || typeof payload.target === "string") {
      return payload;
    }
    return { ...payload, target: topLevelTarget };
  };

  if (record.payload && typeof record.payload === "object" && !Array.isArray(record.payload)) {
    return withTarget(record.payload as Record<string, unknown>);
  }

  if (typeof record.payload === "string") {
    const parsedPayload = parseJson(record.payload);
    if (
      parsedPayload &&
      typeof parsedPayload === "object" &&
      !Array.isArray(parsedPayload)
    ) {
      return withTarget(parsedPayload as Record<string, unknown>);
    }
    return topLevelTarget ? { target: topLevelTarget } : { target: record.payload };
  }

  if (typeof record.query === "string") {
    const parsedQuery = parseJson(record.query);
    if (parsedQuery && typeof parsedQuery === "object" && !Array.isArray(parsedQuery)) {
      return withTarget(parsedQuery as Record<string, unknown>);
    }
    return topLevelTarget ? { target: topLevelTarget } : { target: record.query };
  }

  if (topLevelTarget) {
    return { target: topLevelTarget };
  }

  return {};
}

function resolveVariableTarget(payload: Record<string, unknown>): string {
  const target = payload.target;
  if (typeof target === "string") {
    const parsedTarget = parseJson(target);
    if (
      parsedTarget &&
      typeof parsedTarget === "object" &&
      !Array.isArray(parsedTarget) &&
      typeof (parsedTarget as Record<string, unknown>).target === "string"
    ) {
      return (parsedTarget as Record<string, string>).target;
    }
    return target;
  }
  return "metrics";
}

function monthlyValueMap(rows: FactValueRow[]): Map<string, number> {
  const map = new Map<string, number>();
  rows.forEach((row) => {
    const monthKey = monthKeyFromDate(row.start_date);
    map.set(monthKey, (map.get(monthKey) || 0) + row.value);
  });
  return map;
}

function monthlyBucketMap(rows: FactValueRow[]): Map<string, number[]> {
  const map = new Map<string, number[]>();
  rows.forEach((row) => {
    const monthKey = monthKeyFromDate(row.start_date);
    const existing = map.get(monthKey) || [];
    existing.push(row.value);
    map.set(monthKey, existing);
  });
  return map;
}

function yearlyBucketMap(rows: FactValueRow[]): Map<number, number[]> {
  const map = new Map<number, number[]>();
  rows.forEach((row) => {
    const year = yearFromDate(row.start_date);
    const existing = map.get(year) || [];
    existing.push(row.value);
    map.set(year, existing);
  });
  return map;
}

function monthlyCountMap(rows: FactValueRow[]): Map<string, number> {
  const map = new Map<string, number>();
  rows.forEach((row) => {
    const monthKey = monthKeyFromDate(row.start_date);
    map.set(monthKey, (map.get(monthKey) || 0) + 1);
  });
  return map;
}

function yearlyCountMap(rows: FactValueRow[]): Map<number, number> {
  const map = new Map<number, number>();
  rows.forEach((row) => {
    const year = yearFromDate(row.start_date);
    map.set(year, (map.get(year) || 0) + 1);
  });
  return map;
}

function yearlySumMap(rows: FactValueRow[]): Map<number, number> {
  const map = new Map<number, number>();
  rows.forEach((row) => {
    const year = yearFromDate(row.start_date);
    map.set(year, (map.get(year) || 0) + row.value);
  });
  return map;
}

function pointsFromMonthlyMap(map: Map<string, number>): Array<[number, number]> {
  return [...map.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([month, value]) => [value, monthTimestamp(month)] as [number, number]);
}

function pointsFromYearlyMap(map: Map<number, number>): Array<[number, number]> {
  return [...map.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([year, value]) => [value, yearTimestamp(year)] as [number, number]);
}

function coverageByMonth(
  rows: EventCoverageRow[]
): Map<string, { total: number; hasValue: number }> {
  const map = new Map<string, { total: number; hasValue: number }>();
  rows.forEach((row) => {
    const monthKey = monthKeyFromDate(row.start_date);
    const existing = map.get(monthKey) || { total: 0, hasValue: 0 };
    existing.total += 1;
    existing.hasValue += row.has_value ? 1 : 0;
    map.set(monthKey, existing);
  });
  return map;
}

function coverageByYear(
  rows: EventCoverageRow[]
): Map<number, { total: number; hasValue: number }> {
  const map = new Map<number, { total: number; hasValue: number }>();
  rows.forEach((row) => {
    const year = yearFromDate(row.start_date);
    const existing = map.get(year) || { total: 0, hasValue: 0 };
    existing.total += 1;
    existing.hasValue += row.has_value ? 1 : 0;
    map.set(year, existing);
  });
  return map;
}

function groupMonthlySeriesByType(
  rows: GroupMembershipRow[]
): Map<string, Map<string, number>> {
  const series = new Map<string, Map<string, number>>();

  rows.forEach((row) => {
    if (!row.member_start_date) {
      return;
    }

    const month = monthKeyFromDate(row.member_start_date);
    const seriesKey = `${row.group_name} - ${row.group_member_type_name}`;
    const monthMap = series.get(seriesKey) || new Map<string, number>();
    monthMap.set(month, (monthMap.get(month) || 0) + 1);
    series.set(seriesKey, monthMap);
  });

  return series;
}

function groupYearlySeriesByType(
  rows: GroupMembershipRow[]
): Map<string, Map<number, number>> {
  const series = new Map<string, Map<number, number>>();

  rows.forEach((row) => {
    if (!row.member_start_date) {
      return;
    }

    const year = yearFromDate(row.member_start_date);
    const seriesKey = `${row.group_name} - ${row.group_member_type_name}`;
    const yearMap = series.get(seriesKey) || new Map<number, number>();
    yearMap.set(year, (yearMap.get(year) || 0) + 1);
    series.set(seriesKey, yearMap);
  });

  return series;
}

function groupYearlyTotalSeries(
  rows: GroupMembershipRow[]
): Map<string, Map<number, number>> {
  const series = new Map<string, Map<number, number>>();

  rows.forEach((row) => {
    if (!row.member_start_date) {
      return;
    }

    const year = yearFromDate(row.member_start_date);
    const seriesKey = row.group_name;
    const yearMap = series.get(seriesKey) || new Map<number, number>();
    yearMap.set(year, (yearMap.get(year) || 0) + 1);
    series.set(seriesKey, yearMap);
  });

  return series;
}

function meetingSeriesKey(row: GroupMeetingAttendanceRow): string {
  return `${row.group_name} - ${row.person_name}`;
}

function groupMeetingMonthlyAttendanceBuckets(
  rows: GroupMeetingAttendanceRow[]
): Map<string, Map<string, { present: number; total: number }>> {
  const series = new Map<string, Map<string, { present: number; total: number }>>();

  rows.forEach((row) => {
    const month = monthKeyFromDate(row.start_date);
    const seriesKey = meetingSeriesKey(row);
    const monthMap =
      series.get(seriesKey) || new Map<string, { present: number; total: number }>();
    const bucket = monthMap.get(month) || { present: 0, total: 0 };

    bucket.total += 1;
    if (row.attendance_status === "present") {
      bucket.present += 1;
    }

    monthMap.set(month, bucket);
    series.set(seriesKey, monthMap);
  });

  return series;
}

function groupMeetingYearlyAttendanceBuckets(
  rows: GroupMeetingAttendanceRow[]
): Map<string, Map<number, { present: number; total: number }>> {
  const series = new Map<string, Map<number, { present: number; total: number }>>();

  rows.forEach((row) => {
    const year = yearFromDate(row.start_date);
    const seriesKey = meetingSeriesKey(row);
    const yearMap =
      series.get(seriesKey) || new Map<number, { present: number; total: number }>();
    const bucket = yearMap.get(year) || { present: 0, total: 0 };

    bucket.total += 1;
    if (row.attendance_status === "present") {
      bucket.present += 1;
    }

    yearMap.set(year, bucket);
    series.set(seriesKey, yearMap);
  });

  return series;
}

export const grafanaHandlers = {
  root(c: Context) {
    return c.json({ message: "ChurchTools Facts Grafana JSON Datasource" });
  },

  async metrics(c: Context) {
    try {
      return c.json(buildMetricOptions());
    } catch (error) {
      console.error("Error in metrics endpoint:", error);
      return c.json({ error: "Failed to fetch metrics" }, 500);
    }
  },

  async variable(c: Context) {
    try {
      let body: unknown = {};
      try {
        body = await c.req.json<unknown>();
      } catch {
        body = {};
      }

      const payload = normalizeVariablePayload(body);
      const target = resolveVariableTarget(payload);

      if (target === "event_names") {
        return c.json(
          database.getEventNames().map((name) => ({
            __text: name,
            __value: name,
          }))
        );
      }

      if (target === "calendar_ids") {
        return c.json(
          database.getCalendarIds().map((id) => ({
            __text: `Calendar ${id}`,
            __value: id,
          }))
        );
      }

      if (target === "facts") {
        return c.json(
          database.getNumericFacts().map((fact) => ({
            __text: fact.unit
              ? `${fact.name_translated} (${fact.unit})`
              : fact.name_translated,
            __value: String(fact.id),
          }))
        );
      }

      if (target === "groups") {
        return c.json(
          database.getChurchGroups().map((group) => ({
            __text: group.name,
            __value: String(group.id),
          }))
        );
      }

      if (target === "group_member_types") {
        const groupIds = normalizeNumberList(payload.group_ids);
        return c.json(
          database.getGroupMemberTypes(groupIds).map((type) => ({
            __text: type.name,
            __value: String(type.group_type_role_id),
          }))
        );
      }

      if (target === "group_member_statuses") {
        return c.json(
          GROUP_MEMBER_STATUSES.map((status) => ({
            __text: status,
            __value: status,
          }))
        );
      }

      if (target === "group_meeting_people") {
        const groupIds = normalizeNumberList(payload.group_ids);
        return c.json(
          database.getGroupMeetingPeople(groupIds).map((person) => ({
            __text: person.person_name,
            __value: String(person.person_id),
          }))
        );
      }

      if (target === "group_meeting_attendance_statuses") {
        return c.json(
          GROUP_MEETING_ATTENDANCE_STATUSES.map((status) => ({
            __text: status,
            __value: status,
          }))
        );
      }

      if (target === "metrics" || target.startsWith("metrics_")) {
        const suffix = target === "metrics" ? null : target.replace("metrics_", "");
        const metrics = buildMetricOptions().filter((metric) =>
          suffix ? metric.value.endsWith(`_${suffix}`) : true
        );

        return c.json(
          metrics.map((metric) => ({
            __text: metric.label,
            __value: metric.value,
          }))
        );
      }

      return c.json([]);
    } catch (error) {
      console.error("Error in variable endpoint:", error);
      return c.json([]);
    }
  },

  async query(c: Context) {
    try {
      const body = await c.req.json<GrafanaQueryRequest>();
      const { targets, range } = body;

      const facts = database.getNumericFacts();
      const factsById = new Map<number, Fact>(facts.map((fact) => [fact.id, fact]));

      const valueRowsCache = new Map<string, FactValueRow[]>();
      const coverageRowsCache = new Map<string, EventCoverageRow[]>();

      const from = new Date(range.from).toISOString();
      const to = new Date(range.to).toISOString();
      const monthRange = buildMonthRange(from, to);

      const results: Array<GrafanaDataPoint | GrafanaTableResponse> = [];

      for (const target of targets) {
        if (target.target.startsWith(GROUP_MEETING_ATTENDANCE_TARGET_PREFIX)) {
          const meetingAggregation = target.target.replace(
            GROUP_MEETING_ATTENDANCE_TARGET_PREFIX,
            ""
          );

          if (
            !GROUP_MEETING_ATTENDANCE_AGGREGATION_KEYS.has(
              meetingAggregation as (typeof GROUP_MEETING_ATTENDANCE_AGGREGATION_DEFINITIONS)[number]["key"]
            )
          ) {
            continue;
          }

          const groupIds = normalizeNumberList(target.payload?.group_ids);
          const personIds = normalizeNumberList(target.payload?.group_meeting_person_ids);
          const attendanceStatuses =
            normalizeMeetingAttendanceStatuses(
              target.payload?.group_meeting_attendance_statuses
            ) || DEFAULT_GROUP_MEETING_ATTENDANCE_STATUSES;
          const includeCanceledMeetings = normalizeBoolean(
            target.payload?.include_canceled_meetings,
            false
          );
          const includeIncompleteMeetings = normalizeBoolean(
            target.payload?.include_incomplete_meetings,
            false
          );

          const rows = database.getGroupMeetingAttendancesByDateRange(
            from,
            to,
            groupIds,
            personIds,
            attendanceStatuses,
            includeCanceledMeetings,
            includeIncompleteMeetings
          );

          if (meetingAggregation === "monthly_present_by_person") {
            const seriesBuckets = groupMeetingMonthlyAttendanceBuckets(rows);
            for (const [seriesName, monthMap] of seriesBuckets.entries()) {
              const datapoints = monthRange.map((month) => [
                monthMap.get(month)?.present || 0,
                monthTimestamp(month),
              ]) as Array<[number, number]>;

              results.push(
                toSeries(`Meeting Attendance - ${seriesName} (Present)`, datapoints, "count")
              );
            }
            continue;
          }

          if (meetingAggregation === "monthly_rate_by_person") {
            const seriesBuckets = groupMeetingMonthlyAttendanceBuckets(rows);
            for (const [seriesName, monthMap] of seriesBuckets.entries()) {
              const datapoints = monthRange.map((month) => {
                const bucket = monthMap.get(month);
                const rate = bucket && bucket.total > 0 ? (bucket.present / bucket.total) * 100 : 0;
                return [rate, monthTimestamp(month)] as [number, number];
              });

              results.push(toSeries(`Meeting Attendance Rate - ${seriesName}`, datapoints, "%"));
            }
            continue;
          }

          if (meetingAggregation === "yearly_rate_by_person") {
            const startYear = new Date(from).getUTCFullYear();
            const endYear = new Date(to).getUTCFullYear();
            const years = Array.from(
              { length: endYear - startYear + 1 },
              (_, index) => startYear + index
            );
            const seriesBuckets = groupMeetingYearlyAttendanceBuckets(rows);

            for (const [seriesName, yearMap] of seriesBuckets.entries()) {
              const datapoints = years.map((year) => {
                const bucket = yearMap.get(year);
                const rate = bucket && bucket.total > 0 ? (bucket.present / bucket.total) * 100 : 0;
                return [rate, yearTimestamp(year)] as [number, number];
              });

              results.push(toSeries(`Meeting Attendance Rate - ${seriesName}`, datapoints, "%"));
            }
            continue;
          }

          if (meetingAggregation === "person_summary_table") {
            const summary = new Map<
              string,
              {
                groupName: string;
                personName: string;
                personId: number;
                total: number;
                present: number;
                absent: number;
                unsure: number;
                notInGroup: number;
              }
            >();

            rows.forEach((row) => {
              const key = `${row.group_id}::${row.person_id}`;
              const stats =
                summary.get(key) || {
                  groupName: row.group_name,
                  personName: row.person_name,
                  personId: row.person_id,
                  total: 0,
                  present: 0,
                  absent: 0,
                  unsure: 0,
                  notInGroup: 0,
                };

              stats.total += 1;
              if (row.attendance_status === "present") {
                stats.present += 1;
              } else if (row.attendance_status === "absent") {
                stats.absent += 1;
              } else if (row.attendance_status === "unsure") {
                stats.unsure += 1;
              } else if (row.attendance_status === "not-in-group") {
                stats.notInGroup += 1;
              }

              summary.set(key, stats);
            });

            const tableRows = [...summary.values()]
              .map((stats) => {
                const attendanceRate =
                  stats.total > 0 ? (stats.present / stats.total) * 100 : 0;
                return [
                  stats.groupName,
                  stats.personName,
                  stats.personId,
                  stats.total,
                  stats.present,
                  stats.absent,
                  stats.unsure,
                  stats.notInGroup,
                  attendanceRate,
                ] as Array<string | number>;
              })
              .sort((a, b) => {
                const byGroup = String(a[0]).localeCompare(String(b[0]));
                if (byGroup !== 0) {
                  return byGroup;
                }
                return String(a[1]).localeCompare(String(b[1]));
              });

            results.push({
              type: "table",
              columns: [
                { text: "Group", type: "string" },
                { text: "Person", type: "string" },
                { text: "Person ID", type: "number" },
                { text: "Meetings", type: "number" },
                { text: "Present", type: "number" },
                { text: "Absent", type: "number" },
                { text: "Unsure", type: "number" },
                { text: "Not In Group", type: "number" },
                { text: "Attendance Rate %", type: "number" },
              ],
              rows: tableRows,
            });
            continue;
          }

          continue;
        }

        if (target.target.startsWith(GROUP_MEMBERSHIP_TARGET_PREFIX)) {
          const groupAggregation = target.target.replace(
            GROUP_MEMBERSHIP_TARGET_PREFIX,
            ""
          );

          if (
            !GROUP_MEMBERSHIP_AGGREGATION_KEYS.has(
              groupAggregation as (typeof GROUP_MEMBERSHIP_AGGREGATION_DEFINITIONS)[number]["key"]
            )
          ) {
            continue;
          }

          const groupIds = normalizeNumberList(target.payload?.group_ids);
          const groupTypeRoleIds = normalizeNumberList(
            target.payload?.group_type_role_ids
          );
          const groupMemberStatuses = normalizeStringList(
            target.payload?.group_member_statuses
          );

          const rows = database.getGroupMembershipsByStartDateRange(
            from,
            to,
            groupIds,
            groupTypeRoleIds,
            groupMemberStatuses
          );

          if (groupAggregation === "monthly_by_type") {
            const seriesByType = groupMonthlySeriesByType(rows);
            for (const [seriesName, monthMap] of seriesByType.entries()) {
              const datapoints = monthRange.map((month) =>
                [monthMap.get(month) || 0, monthTimestamp(month)] as [number, number]
              );

              results.push(
                toSeries(`Membership Starts - ${seriesName}`, datapoints, "count")
              );
            }
            continue;
          }

          const startYear = new Date(from).getUTCFullYear();
          const endYear = new Date(to).getUTCFullYear();
          const years = Array.from(
            { length: endYear - startYear + 1 },
            (_, index) => startYear + index
          );

          if (groupAggregation === "yearly_by_type") {
            const seriesByType = groupYearlySeriesByType(rows);
            for (const [seriesName, yearMap] of seriesByType.entries()) {
              const datapoints = years.map((year) =>
                [yearMap.get(year) || 0, yearTimestamp(year)] as [number, number]
              );

              results.push(
                toSeries(`Membership Starts - ${seriesName}`, datapoints, "count")
              );
            }
            continue;
          }

          if (groupAggregation === "yearly_total") {
            const yearlyTotals = groupYearlyTotalSeries(rows);
            for (const [groupName, yearMap] of yearlyTotals.entries()) {
              const datapoints = years.map((year) =>
                [yearMap.get(year) || 0, yearTimestamp(year)] as [number, number]
              );

              results.push(
                toSeries(
                  `Membership Starts - ${groupName} (Total)`,
                  datapoints,
                  "count"
                )
              );
            }
            continue;
          }

          if (groupAggregation === "yearly_yoy_by_type") {
            const seriesByType = groupYearlySeriesByType(rows);
            for (const [seriesName, yearMap] of seriesByType.entries()) {
              const datapoints: Array<[number, number]> = [];

              for (let year = startYear + 1; year <= endYear; year++) {
                const currentCount = yearMap.get(year) || 0;
                const previousCount = yearMap.get(year - 1) || 0;
                datapoints.push([
                  currentCount - previousCount,
                  yearTimestamp(year),
                ]);
              }

              if (datapoints.length > 0) {
                results.push(
                  toSeries(
                    `Membership Starts YoY - ${seriesName}`,
                    datapoints,
                    "count"
                  )
                );
              }
            }
            continue;
          }

          if (groupAggregation === "compare_table") {
            const yearlyCountsBySeries = new Map<string, Map<number, number>>();

            rows.forEach((row) => {
              if (!row.member_start_date) {
                return;
              }

              const year = yearFromDate(row.member_start_date);
              const seriesKey = [
                row.group_name,
                row.group_member_type_name,
              ].join("::");

              const yearlyCounts =
                yearlyCountsBySeries.get(seriesKey) || new Map<number, number>();
              yearlyCounts.set(year, (yearlyCounts.get(year) || 0) + 1);
              yearlyCountsBySeries.set(seriesKey, yearlyCounts);
            });

            const tableRows = [...yearlyCountsBySeries.entries()]
              .flatMap(([seriesKey, yearlyCounts]) => {
                const [groupName, memberTypeName] = seriesKey.split("::");
                return [...yearlyCounts.entries()].map(([year, count]) => {
                  const previousCount = yearlyCounts.get(year - 1);
                  const yoyDelta =
                    typeof previousCount === "number" ? count - previousCount : null;
                  const yoyPercent =
                    typeof previousCount === "number" && previousCount !== 0
                      ? ((count - previousCount) / previousCount) * 100
                      : null;

                  return [
                    groupName,
                    memberTypeName,
                    year,
                    count,
                    previousCount ?? null,
                    yoyDelta,
                    yoyPercent,
                  ] as Array<string | number | null>;
                });
              })
              .sort((a, b) => {
                const byGroup = String(a[0]).localeCompare(String(b[0]));
                if (byGroup !== 0) {
                  return byGroup;
                }

                const byType = String(a[1]).localeCompare(String(b[1]));
                if (byType !== 0) {
                  return byType;
                }

                return Number(a[2]) - Number(b[2]);
              });

            results.push({
              type: "table",
              columns: [
                { text: "Group", type: "string" },
                { text: "Group Member Type", type: "string" },
                { text: "Year", type: "number" },
                { text: "Membership Starts", type: "number" },
                { text: "Previous Year", type: "number" },
                { text: "YoY Delta", type: "number" },
                { text: "YoY %", type: "number" },
              ],
              rows: tableRows,
            });
            continue;
          }

          continue;
        }

        const match = target.target.match(/^fact_(\d+)_(.+)$/);
        if (!match) {
          continue;
        }

        const factId = Number(match[1]);
        const aggregation = match[2];

        if (
          !FACT_AGGREGATION_KEYS.has(
            aggregation as (typeof FACT_AGGREGATION_DEFINITIONS)[number]["key"]
          )
        ) {
          continue;
        }

        const fact = factsById.get(factId);
        if (!fact) {
          continue;
        }

        const eventNames = normalizeStringList(target.payload?.event_names);
        const calendarIds = normalizeNumberList(target.payload?.calendar_ids);
        const denominatorFactId = normalizeNumberList(target.payload?.denominator_fact_id)?.[0];
        const topN = normalizePositiveInteger(target.payload?.top_n, 5);

        const filterLabel = buildFilterLabel(eventNames, calendarIds);
        const baseName = fact.unit
          ? `${fact.name_translated} (${fact.unit})`
          : fact.name_translated;

        const valueCacheKey = [
          factId,
          from,
          to,
          (eventNames || []).join("|"),
          (calendarIds || []).join("|"),
        ].join("::");

        const getValueRows = () => {
          if (!valueRowsCache.has(valueCacheKey)) {
            valueRowsCache.set(
              valueCacheKey,
              database.getFactValuesByTimeRange(factId, from, to, eventNames, calendarIds)
            );
          }
          return valueRowsCache.get(valueCacheKey) || [];
        };

        const getCoverageRows = () => {
          if (!coverageRowsCache.has(valueCacheKey)) {
            coverageRowsCache.set(
              valueCacheKey,
              database.getEventCoverageByTimeRange(
                factId,
                from,
                to,
                eventNames,
                calendarIds
              )
            );
          }
          return coverageRowsCache.get(valueCacheKey) || [];
        };

        if (aggregation === "raw") {
          const rows = getValueRows().sort((a, b) =>
            new Date(a.start_date).getTime() - new Date(b.start_date).getTime()
          );

          const datapointsByEventName = new Map<string, Array<[number, number]>>();
          rows.forEach((row) => {
            const eventName = row.event_name_full || row.event_name || "Unknown Event";
            const datapoints = datapointsByEventName.get(eventName) || [];
            datapoints.push([row.value, new Date(row.start_date).getTime()]);
            datapointsByEventName.set(eventName, datapoints);
          });

          for (const [eventName, datapoints] of datapointsByEventName.entries()) {
            results.push(
              toSeries(`${baseName} - Raw (${eventName})${filterLabel}`, datapoints, fact.unit)
            );
          }
          continue;
        }

        if (aggregation === "monthly") {
          const datapoints = pointsFromMonthlyMap(monthlyValueMap(getValueRows()));
          if (datapoints.length > 0) {
            results.push(toSeries(`${baseName} - Monthly Sum${filterLabel}`, datapoints, fact.unit));
          }
          continue;
        }

        if (aggregation === "monthly_ratio") {
          if (!denominatorFactId || !factsById.has(denominatorFactId)) {
            continue;
          }

          const denominatorFact = factsById.get(denominatorFactId)!;
          const denominatorRows = database.getFactValuesByTimeRange(
            denominatorFactId,
            from,
            to,
            eventNames,
            calendarIds
          );

          const numeratorMap = monthlyValueMap(getValueRows());
          const denominatorMap = monthlyValueMap(denominatorRows);

          const datapoints = monthRange.map((month) => {
            const numerator = numeratorMap.get(month) || 0;
            const denominator = denominatorMap.get(month) || 0;
            const ratio = denominator === 0 ? 0 : numerator / denominator;
            return [ratio, monthTimestamp(month)] as [number, number];
          });

          const denominatorName = denominatorFact.unit
            ? `${denominatorFact.name_translated} (${denominatorFact.unit})`
            : denominatorFact.name_translated;

          results.push(
            toSeries(
              `${baseName} / ${denominatorName} - Monthly Ratio${filterLabel}`,
              datapoints,
              "ratio"
            )
          );
          continue;
        }

        if (aggregation === "yearly_sum") {
          const datapoints = pointsFromYearlyMap(yearlySumMap(getValueRows()));
          if (datapoints.length > 0) {
            results.push(toSeries(`${baseName} - Yearly Sum${filterLabel}`, datapoints, fact.unit));
          }
          continue;
        }

        if (aggregation === "yearly_mean") {
          const yearlyMeans = new Map<number, number>();
          yearlyBucketMap(getValueRows()).forEach((values, year) => {
            yearlyMeans.set(year, average(values));
          });

          const datapoints = pointsFromYearlyMap(yearlyMeans);
          if (datapoints.length > 0) {
            results.push(toSeries(`${baseName} - Yearly Mean${filterLabel}`, datapoints, fact.unit));
          }
          continue;
        }

        if (aggregation === "monthly_count") {
          const datapoints = pointsFromMonthlyMap(monthlyCountMap(getValueRows()));
          if (datapoints.length > 0) {
            results.push(toSeries(`${baseName} - Monthly Event Count${filterLabel}`, datapoints, "count"));
          }
          continue;
        }

        if (aggregation === "yearly_count") {
          const datapoints = pointsFromYearlyMap(yearlyCountMap(getValueRows()));
          if (datapoints.length > 0) {
            results.push(toSeries(`${baseName} - Yearly Event Count${filterLabel}`, datapoints, "count"));
          }
          continue;
        }

        if (aggregation === "monthly_rolling_3" || aggregation === "monthly_rolling_6") {
          const windowSize = aggregation === "monthly_rolling_3" ? 3 : 6;
          const sumMap = monthlyValueMap(getValueRows());
          const values = monthRange.map((month) => sumMap.get(month) || 0);

          const datapoints = monthRange.map((month, index) => {
            const windowStart = Math.max(0, index - windowSize + 1);
            const window = values.slice(windowStart, index + 1);
            return [average(window), monthTimestamp(month)] as [number, number];
          });

          results.push(
            toSeries(
              `${baseName} - Monthly Rolling Average (${windowSize}M)${filterLabel}`,
              datapoints,
              fact.unit
            )
          );
          continue;
        }

        if (aggregation === "ytd_cumulative") {
          const sumMap = monthlyValueMap(getValueRows());
          const yearlyRunning = new Map<number, number>();

          const datapoints = monthRange.map((month) => {
            const year = Number(month.slice(0, 4));
            const next = (yearlyRunning.get(year) || 0) + (sumMap.get(month) || 0);
            yearlyRunning.set(year, next);
            return [next, monthTimestamp(month)] as [number, number];
          });

          results.push(toSeries(`${baseName} - YTD Cumulative${filterLabel}`, datapoints, fact.unit));
          continue;
        }

        if (aggregation === "yearly_yoy_delta" || aggregation === "yearly_yoy_percent") {
          const sums = yearlySumMap(getValueRows());
          const startYear = new Date(from).getUTCFullYear();
          const endYear = new Date(to).getUTCFullYear();

          const datapoints: Array<[number, number]> = [];
          for (let year = startYear + 1; year <= endYear; year++) {
            const current = sums.get(year) || 0;
            const previous = sums.get(year - 1) || 0;

            if (aggregation === "yearly_yoy_delta") {
              datapoints.push([current - previous, yearTimestamp(year)]);
            } else if (previous !== 0) {
              const yoyPercent = ((current - previous) / previous) * 100;
              datapoints.push([yoyPercent, yearTimestamp(year)]);
            }
          }

          if (datapoints.length > 0) {
            const label =
              aggregation === "yearly_yoy_delta" ? "Yearly YoY Delta" : "Yearly YoY Percent";
            const unit = aggregation === "yearly_yoy_delta" ? fact.unit : "%";
            results.push(toSeries(`${baseName} - ${label}${filterLabel}`, datapoints, unit));
          }
          continue;
        }

        if (aggregation === "monthly_distribution" || aggregation === "yearly_distribution") {
          const isMonthly = aggregation === "monthly_distribution";

          if (isMonthly) {
            const buckets = monthlyBucketMap(getValueRows());
            const minPoints: Array<[number, number]> = [];
            const medianPoints: Array<[number, number]> = [];
            const p90Points: Array<[number, number]> = [];
            const maxPoints: Array<[number, number]> = [];

            [...buckets.entries()]
              .sort((a, b) => a[0].localeCompare(b[0]))
              .forEach(([month, values]) => {
                const ts = monthTimestamp(month);
                minPoints.push([Math.min(...values), ts]);
                medianPoints.push([percentile(values, 50), ts]);
                p90Points.push([percentile(values, 90), ts]);
                maxPoints.push([Math.max(...values), ts]);
              });

            results.push(toSeries(`${baseName} - Monthly Min${filterLabel}`, minPoints, fact.unit));
            results.push(
              toSeries(`${baseName} - Monthly Median${filterLabel}`, medianPoints, fact.unit)
            );
            results.push(toSeries(`${baseName} - Monthly P90${filterLabel}`, p90Points, fact.unit));
            results.push(toSeries(`${baseName} - Monthly Max${filterLabel}`, maxPoints, fact.unit));
          } else {
            const buckets = yearlyBucketMap(getValueRows());
            const minPoints: Array<[number, number]> = [];
            const medianPoints: Array<[number, number]> = [];
            const p90Points: Array<[number, number]> = [];
            const maxPoints: Array<[number, number]> = [];

            [...buckets.entries()]
              .sort((a, b) => a[0] - b[0])
              .forEach(([year, values]) => {
                const ts = yearTimestamp(year);
                minPoints.push([Math.min(...values), ts]);
                medianPoints.push([percentile(values, 50), ts]);
                p90Points.push([percentile(values, 90), ts]);
                maxPoints.push([Math.max(...values), ts]);
              });

            results.push(toSeries(`${baseName} - Yearly Min${filterLabel}`, minPoints, fact.unit));
            results.push(
              toSeries(`${baseName} - Yearly Median${filterLabel}`, medianPoints, fact.unit)
            );
            results.push(toSeries(`${baseName} - Yearly P90${filterLabel}`, p90Points, fact.unit));
            results.push(toSeries(`${baseName} - Yearly Max${filterLabel}`, maxPoints, fact.unit));
          }
          continue;
        }

        if (aggregation === "monthly_anomaly_band") {
          const sumMap = monthlyValueMap(getValueRows());
          const values = monthRange.map((month) => sumMap.get(month) || 0);

          const mainPoints: Array<[number, number]> = [];
          const lowerPoints: Array<[number, number]> = [];
          const upperPoints: Array<[number, number]> = [];

          values.forEach((value, index) => {
            const windowStart = Math.max(0, index - 11);
            const window = values.slice(windowStart, index + 1);
            const mean = average(window);
            const variance = average(window.map((entry) => (entry - mean) ** 2));
            const stddev = Math.sqrt(variance);
            const ts = monthTimestamp(monthRange[index]);

            mainPoints.push([value, ts]);
            lowerPoints.push([mean - stddev, ts]);
            upperPoints.push([mean + stddev, ts]);
          });

          results.push(toSeries(`${baseName} - Monthly Sum${filterLabel}`, mainPoints, fact.unit));
          results.push(toSeries(`${baseName} - Anomaly Lower${filterLabel}`, lowerPoints, fact.unit));
          results.push(toSeries(`${baseName} - Anomaly Upper${filterLabel}`, upperPoints, fact.unit));
          continue;
        }

        if (aggregation === "monthly_by_event") {
          const rows = getValueRows();
          if (rows.length === 0) {
            continue;
          }

          const totals = new Map<string, number>();
          rows.forEach((row) => {
            const eventName = row.event_name || "Unknown";
            totals.set(eventName, (totals.get(eventName) || 0) + row.value);
          });

          const seriesEvents =
            eventNames && eventNames.length > 0
              ? eventNames
              : [...totals.entries()]
                  .sort((a, b) => b[1] - a[1])
                  .slice(0, 5)
                  .map(([name]) => name);

          seriesEvents.forEach((eventName) => {
            const monthlyMap = new Map<string, number>();
            rows
              .filter((row) => (row.event_name || "Unknown") === eventName)
              .forEach((row) => {
                const month = monthKeyFromDate(row.start_date);
                monthlyMap.set(month, (monthlyMap.get(month) || 0) + row.value);
              });

            const datapoints = monthRange.map((month) => [
              monthlyMap.get(month) || 0,
              monthTimestamp(month),
            ]) as Array<[number, number]>;

            results.push(toSeries(`${baseName} - ${eventName}${filterLabel}`, datapoints, fact.unit));
          });
          continue;
        }

        if (aggregation === "monthly_by_calendar") {
          const rows = getValueRows();
          if (rows.length === 0) {
            continue;
          }

          const totals = new Map<number, number>();
          rows.forEach((row) => {
            const calendarId = row.calendar_id ?? -1;
            totals.set(calendarId, (totals.get(calendarId) || 0) + row.value);
          });

          const seriesCalendars =
            calendarIds && calendarIds.length > 0
              ? calendarIds
              : [...totals.entries()]
                  .sort((a, b) => b[1] - a[1])
                  .slice(0, 5)
                  .map(([calendarId]) => calendarId);

          seriesCalendars.forEach((calendarId) => {
            const monthlyMap = new Map<string, number>();
            rows
              .filter((row) => (row.calendar_id ?? -1) === calendarId)
              .forEach((row) => {
                const month = monthKeyFromDate(row.start_date);
                monthlyMap.set(month, (monthlyMap.get(month) || 0) + row.value);
              });

            const datapoints = monthRange.map((month) => [
              monthlyMap.get(month) || 0,
              monthTimestamp(month),
            ]) as Array<[number, number]>;

            const calendarLabel = calendarId === -1 ? "No Calendar" : `Calendar ${calendarId}`;
            results.push(toSeries(`${baseName} - ${calendarLabel}${filterLabel}`, datapoints, fact.unit));
          });
          continue;
        }

        if (aggregation === "monthly_completeness") {
          const map = coverageByMonth(getCoverageRows());
          const datapoints = [...map.entries()]
            .sort((a, b) => a[0].localeCompare(b[0]))
            .map(([month, coverage]) => [
              coverage.total > 0 ? (coverage.hasValue / coverage.total) * 100 : 0,
              monthTimestamp(month),
            ]) as Array<[number, number]>;

          if (datapoints.length > 0) {
            results.push(toSeries(`${baseName} - Monthly Completeness${filterLabel}`, datapoints, "%"));
          }
          continue;
        }

        if (aggregation === "yearly_completeness") {
          const map = coverageByYear(getCoverageRows());
          const datapoints = [...map.entries()]
            .sort((a, b) => a[0] - b[0])
            .map(([year, coverage]) => [
              coverage.total > 0 ? (coverage.hasValue / coverage.total) * 100 : 0,
              yearTimestamp(year),
            ]) as Array<[number, number]>;

          if (datapoints.length > 0) {
            results.push(toSeries(`${baseName} - Yearly Completeness${filterLabel}`, datapoints, "%"));
          }
          continue;
        }

        if (aggregation === "top_bottom_table") {
          const rows = getValueRows();
          if (rows.length === 0) {
            continue;
          }

          const sortedDescending = [...rows].sort((a, b) => b.value - a.value).slice(0, topN);
          const sortedAscending = [...rows].sort((a, b) => a.value - b.value).slice(0, topN);

          const tableRows: Array<Array<string | number | null>> = [];

          sortedDescending.forEach((row) => {
            tableRows.push([
              "Top",
              row.event_name_full,
              row.event_name || "",
              row.calendar_id ?? null,
              new Date(row.start_date).getTime(),
              row.value,
            ]);
          });

          sortedAscending.forEach((row) => {
            tableRows.push([
              "Bottom",
              row.event_name_full,
              row.event_name || "",
              row.calendar_id ?? null,
              new Date(row.start_date).getTime(),
              row.value,
            ]);
          });

          results.push({
            type: "table",
            columns: [
              { text: "Direction", type: "string" },
              { text: "Event", type: "string" },
              { text: "Event Type", type: "string" },
              { text: "Calendar ID", type: "number" },
              { text: "Start Date", type: "time" },
              { text: "Value", type: "number" },
            ],
            rows: tableRows,
          });
          continue;
        }
      }

      return c.json(results);
    } catch (error) {
      console.error("Error in query endpoint:", error);
      return c.json({ error: "Failed to query data" }, 500);
    }
  },

  annotations(c: Context) {
    return c.json([]);
  },

  tagKeys(c: Context) {
    return c.json([
      { type: "string", text: "event_name" },
      { type: "number", text: "calendar_id" },
      { type: "number", text: "group_id" },
      { type: "number", text: "group_type_role_id" },
      { type: "string", text: "group_member_status" },
      { type: "number", text: "group_meeting_person_id" },
      { type: "string", text: "group_meeting_attendance_status" },
    ]);
  },

  async tagValues(c: Context) {
    try {
      const body = await c.req.json<{ key: string }>();

      if (body.key === "event_name") {
        return c.json(database.getEventNames().map((name) => ({ text: name })));
      }

      if (body.key === "calendar_id") {
        return c.json(database.getCalendarIds().map((id) => ({ text: String(id) })));
      }

      if (body.key === "group_id") {
        return c.json(
          database
            .getChurchGroups()
            .map((group) => ({ text: `${group.id} - ${group.name}` }))
        );
      }

      if (body.key === "group_type_role_id") {
        return c.json(
          database
            .getGroupMemberTypes()
            .map((type) => ({ text: `${type.group_type_role_id} - ${type.name}` }))
        );
      }

      if (body.key === "group_member_status") {
        return c.json(GROUP_MEMBER_STATUSES.map((status) => ({ text: status })));
      }

      if (body.key === "group_meeting_person_id") {
        return c.json(
          database
            .getGroupMeetingPeople()
            .map((person) => ({ text: `${person.person_id} - ${person.person_name}` }))
        );
      }

      if (body.key === "group_meeting_attendance_status") {
        return c.json(
          GROUP_MEETING_ATTENDANCE_STATUSES.map((status) => ({ text: status }))
        );
      }

      return c.json([]);
    } catch (error) {
      console.error("Error in tag-values endpoint:", error);
      return c.json([]);
    }
  },
};
