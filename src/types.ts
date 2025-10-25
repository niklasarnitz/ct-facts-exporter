// ChurchTools API Types
export interface CTFact {
  id: number;
  name: string;
  nameTranslated: string;
  type: "number" | "select";
  unit?: string;
  sortKey: number;
  options?: string[];
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
  serviceGroups?: any[];
  services?: any[];
}

// Database Types
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
  event_name: string;
  start_date: string;
  end_date?: string;
  fact_name: string;
  unit?: string;
}

// Grafana JSON Datasource Types
export interface GrafanaQueryTarget {
  target: string;
  refId: string;
  type: string;
  data?: any;
}

export interface GrafanaTimeRange {
  from: string;
  to: string;
}

export interface GrafanaQueryRequest {
  targets: GrafanaQueryTarget[];
  range: GrafanaTimeRange;
  intervalMs?: number;
  maxDataPoints?: number;
}

export interface GrafanaDataPoint {
  target: string;
  datapoints: Array<[number, number]>; // [value, timestamp_ms]
}

export interface GrafanaSearchResult {
  text: string;
  value: number | string;
}

export interface GrafanaAnnotation {
  time: number;
  timeEnd?: number;
  title: string;
  text: string;
  tags?: string[];
}
