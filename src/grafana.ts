import { Context } from "hono";
import { database } from "./db";

const FILTER_PAYLOAD_CONFIG = {
  name: "event_names",
  label: "Filter by Event Names",
  type: "multi-select" as const,
  placeholder: "Select event names to filter (optional)",
};

const AGGREGATIONS = ["raw", "monthly", "yearly_sum", "yearly_mean"] as const;
const VARIABLE_TARGETS = new Set<string>([
  "metrics",
  "metrics_raw",
  "metrics_monthly",
  "metrics_yearly_sum",
  "metrics_yearly_mean",
  "event_names",
]);
const ALL_VALUE_TOKENS = new Set(["__all", "$__all", "all", "All", "*"]);

export interface GrafanaQueryRequest {
  targets: Array<{
    target: string;
    refId: string;
    type?: string;
    payload?: {
      aggregation?: "raw" | "monthly" | "yearly_sum" | "yearly_mean";
      event_names?: string[] | string;
      [key: string]: any;
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
  datapoints: Array<[number, number]>; // [value, timestamp_ms]
  unit?: string;
}

export interface GrafanaMetricOption {
  label: string;
  value: string;
  payloads?: Array<{
    name: string;
    label?: string;
    type: "select" | "multi-select" | "input";
    placeholder?: string;
    options?: Array<{ label: string; value: string }>;
  }>;
}

function parseJson(input: string): unknown {
  try {
    return JSON.parse(input);
  } catch {
    return undefined;
  }
}

function normalizeEventNames(value: unknown): string[] | undefined {
  if (value === null || value === undefined) {
    return undefined;
  }

  if (Array.isArray(value)) {
    const normalized = value
      .flatMap((item) => normalizeEventNames(item) || [])
      .filter((item) => item.length > 0)
      .filter((item) => !ALL_VALUE_TOKENS.has(item));
    return normalized.length > 0 ? normalized : undefined;
  }

  if (typeof value !== "string") {
    return undefined;
  }

  const trimmed = value.trim();
  if (!trimmed) {
    return undefined;
  }

  const parsed = parseJson(trimmed);
  if (parsed !== undefined) {
    return normalizeEventNames(parsed);
  }

  if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
    const tokens = trimmed
      .slice(1, -1)
      .split(",")
      .map((token) => token.trim())
      .filter((token) => token.length > 0)
      .filter((token) => !ALL_VALUE_TOKENS.has(token));
    return tokens.length > 0 ? tokens : undefined;
  }

  if (trimmed.includes(",")) {
    const tokens = trimmed
      .split(",")
      .map((token) => token.trim())
      .filter((token) => token.length > 0)
      .filter((token) => !ALL_VALUE_TOKENS.has(token));
    return tokens.length > 0 ? tokens : undefined;
  }

  if (ALL_VALUE_TOKENS.has(trimmed)) {
    return undefined;
  }

  return [trimmed];
}

function buildMetricOptions(): GrafanaMetricOption[] {
  const facts = database.getNumericFacts();
  const eventNames = database.getEventNames();

  return facts.flatMap((fact) => {
    const label = fact.unit
      ? `${fact.name_translated} (${fact.unit})`
      : fact.name_translated;

    return AGGREGATIONS.map((aggregation) => {
      const aggregationLabel = {
        raw: "Raw Data",
        monthly: "Monthly Sum",
        yearly_sum: "Yearly Sum",
        yearly_mean: "Yearly Mean",
      }[aggregation];

      return {
        label: `${label} - ${aggregationLabel}`,
        value: `fact_${fact.id}_${aggregation}`,
        payloads: [
          {
            ...FILTER_PAYLOAD_CONFIG,
            options: eventNames.map((name) => ({ label: name, value: name })),
          },
        ],
      };
    });
  });
}

function normalizeVariablePayload(body: unknown): Record<string, unknown> {
  if (typeof body === "string") {
    const parsedBody = parseJson(body);
    if (typeof parsedBody === "object" && parsedBody !== null) {
      return normalizeVariablePayload(parsedBody);
    }
    return { target: body };
  }

  if (!body || typeof body !== "object" || Array.isArray(body)) {
    return {};
  }

  const record = body as Record<string, unknown>;
  const payload = record.payload;

  if (payload && typeof payload === "object" && !Array.isArray(payload)) {
    return payload as Record<string, unknown>;
  }

  if (typeof payload === "string") {
    const parsedPayload = parseJson(payload);
    if (
      parsedPayload &&
      typeof parsedPayload === "object" &&
      !Array.isArray(parsedPayload)
    ) {
      return parsedPayload as Record<string, unknown>;
    }
    return { target: payload };
  }

  if (typeof record.target === "string") {
    return { target: record.target };
  }

  if (typeof record.query === "string") {
    const parsedQuery = parseJson(record.query);
    if (
      parsedQuery &&
      typeof parsedQuery === "object" &&
      !Array.isArray(parsedQuery)
    ) {
      return parsedQuery as Record<string, unknown>;
    }
    return { target: record.query };
  }

  return {};
}

function resolveVariableTarget(payload: Record<string, unknown>): string {
  const rawTarget = payload.target;

  if (typeof rawTarget === "string") {
    const parsedTarget = parseJson(rawTarget);
    if (
      parsedTarget &&
      typeof parsedTarget === "object" &&
      !Array.isArray(parsedTarget)
    ) {
      const nestedTarget = (parsedTarget as Record<string, unknown>).target;
      if (typeof nestedTarget === "string") {
        return nestedTarget;
      }
    }
    return rawTarget;
  }

  return "metrics";
}

export const grafanaHandlers = {
  // Health check endpoint
  root(c: Context) {
    return c.json({ message: "ChurchTools Facts Grafana JSON Datasource" });
  },

  // Metrics endpoint - returns available metrics with payloads
  async metrics(c: Context) {
    try {
      return c.json(buildMetricOptions());
    } catch (error) {
      console.error("Error in metrics endpoint:", error);
      return c.json({ error: "Failed to fetch metrics" }, 500);
    }
  },

  // Variable endpoint - supports Grafana query variables
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

      if (!VARIABLE_TARGETS.has(target)) {
        return c.json([]);
      }

      if (target === "event_names") {
        const eventNames = database.getEventNames();
        return c.json(
          eventNames.map((name) => ({
            __text: name,
            __value: name,
          }))
        );
      }

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
    } catch (error) {
      console.error("Error in variable endpoint:", error);
      return c.json([]);
    }
  },

  // Query endpoint - returns time series data
  async query(c: Context) {
    try {
      const body = await c.req.json<GrafanaQueryRequest>();
      const { targets, range } = body;

      const results: GrafanaDataPoint[] = [];

      for (const target of targets) {
        const targetValue = target.target;
        const eventNames = normalizeEventNames(target.payload?.event_names);

        // Parse target: fact_<id>_<type>
        const match = targetValue.match(
          /^fact_(\d+)_(raw|monthly|yearly_sum|yearly_mean)$/
        );
        if (!match) {
          continue;
        }

        const factId = parseInt(match[1]);
        const aggregation = match[2];

        // Convert ISO strings to SQLite format
        const from = new Date(range.from).toISOString();
        const to = new Date(range.to).toISOString();

        let datapoints: Array<[number, number]> = [];
        let targetName = `Fact ${factId}`;
        let unit: string | undefined = undefined;

        if (aggregation === "raw") {
          const eventFacts =
            eventNames && eventNames.length > 0
              ? database.getEventFactsByTimeRangeFiltered(
                  factId,
                  from,
                  to,
                  eventNames
                )
              : database.getEventFactsByTimeRange(factId, from, to);

          datapoints = eventFacts
            .filter((ef) => ef.value !== null && ef.value !== undefined)
            .map((ef) => {
              const timestamp = new Date(ef.start_date).getTime();
              return [ef.value!, timestamp] as [number, number];
            })
            .sort((a, b) => a[1] - b[1]);

          if (datapoints.length > 0) {
            const factName = eventFacts[0]?.fact_name || `Fact ${factId}`;
            unit = eventFacts[0]?.unit;
            targetName = unit ? `${factName} (${unit})` : factName;
            if (eventNames && eventNames.length > 0) {
              targetName += ` [${eventNames.join(", ")}]`;
            }
          }
        } else if (aggregation === "monthly") {
          const monthlyData = database.getMonthlyAggregateByFact(
            factId,
            from,
            to,
            eventNames
          );

          datapoints = monthlyData.map((d) => {
            // Parse month string "YYYY-MM" and set to first day of month
            const [year, month] = d.month.split("-").map(Number);
            const timestamp = new Date(year, month - 1, 1).getTime();
            return [d.value, timestamp] as [number, number];
          });

          const facts = database.getNumericFacts();
          const fact = facts.find((f) => f.id === factId);
          if (fact) {
            unit = fact.unit;
            targetName = `${fact.name_translated} - Monthly Sum`;
            if (fact.unit) targetName += ` (${fact.unit})`;
            if (eventNames && eventNames.length > 0) {
              targetName += ` [${eventNames.join(", ")}]`;
            }
          }
        } else if (aggregation === "yearly_sum") {
          // Calculate yearly sums for each year in range
          const fromDate = new Date(from);
          const toDate = new Date(to);
          const startYear = fromDate.getFullYear();
          const endYear = toDate.getFullYear();

          for (let year = startYear; year <= endYear; year++) {
            const yearlyData = database.getYearlySum(factId, year, eventNames);
            if (yearlyData.count > 0) {
              const timestamp = new Date(year, 0, 1).getTime();
              datapoints.push([yearlyData.value, timestamp] as [
                number,
                number
              ]);
            }
          }

          const facts = database.getNumericFacts();
          const fact = facts.find((f) => f.id === factId);
          if (fact) {
            unit = fact.unit;
            targetName = `${fact.name_translated} - Yearly Sum`;
            if (fact.unit) targetName += ` (${fact.unit})`;
            if (eventNames && eventNames.length > 0) {
              targetName += ` [${eventNames.join(", ")}]`;
            }
          }
        } else if (aggregation === "yearly_mean") {
          // Calculate yearly means for each year in range
          const fromDate = new Date(from);
          const toDate = new Date(to);
          const startYear = fromDate.getFullYear();
          const endYear = toDate.getFullYear();

          for (let year = startYear; year <= endYear; year++) {
            const yearlyData = database.getYearlyMean(factId, year, eventNames);
            if (yearlyData.count > 0) {
              const timestamp = new Date(year, 0, 1).getTime();
              datapoints.push([yearlyData.value, timestamp] as [
                number,
                number
              ]);
            }
          }

          const facts = database.getNumericFacts();
          const fact = facts.find((f) => f.id === factId);
          if (fact) {
            unit = fact.unit;
            targetName = `${fact.name_translated} - Yearly Mean`;
            if (fact.unit) targetName += ` (${fact.unit})`;
            if (eventNames && eventNames.length > 0) {
              targetName += ` [${eventNames.join(", ")}]`;
            }
          }
        }

        if (datapoints.length > 0) {
          const result: GrafanaDataPoint = {
            target: targetName,
            datapoints,
          };
          if (unit) {
            result.unit = unit;
          }
          results.push(result);
        }
      }

      return c.json(results);
    } catch (error) {
      console.error("Error in query endpoint:", error);
      return c.json({ error: "Failed to query data" }, 500);
    }
  },

  // Annotations endpoint (optional, not implemented yet)
  annotations(c: Context) {
    return c.json([]);
  },

  // Tag keys endpoint (optional)
  tagKeys(c: Context) {
    return c.json([{ type: "string", text: "event_name" }]);
  },

  // Tag values endpoint (optional)
  async tagValues(c: Context) {
    try {
      const body = await c.req.json<{ key: string }>();

      if (body.key === "event_name") {
        const eventNames = database.getEventNames();
        return c.json(eventNames.map((name) => ({ text: name })));
      }

      return c.json([]);
    } catch (error) {
      console.error("Error in tag-values endpoint:", error);
      return c.json([]);
    }
  },
};
