import { Context } from "hono";
import { database } from "./db";

export interface GrafanaQueryRequest {
  targets: Array<{
    target: string;
    refId: string;
    type?: string;
    payload?: {
      aggregation?: "raw" | "monthly" | "yearly_sum" | "yearly_mean";
      event_names?: string[];
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

export const grafanaHandlers = {
  // Health check endpoint
  root(c: Context) {
    return c.json({ message: "ChurchTools Facts Grafana JSON Datasource" });
  },

  // Metrics endpoint - returns available metrics with payloads
  async metrics(c: Context) {
    try {
      const facts = database.getNumericFacts();
      const eventNames = database.getEventNames();

      const metrics: GrafanaMetricOption[] = [];

      // Add raw fact metrics
      facts.forEach((fact) => {
        const label = fact.unit
          ? `${fact.name_translated} (${fact.unit})`
          : fact.name_translated;

        metrics.push({
          label: `${label} - Raw Data`,
          value: `fact_${fact.id}_raw`,
          payloads: [
            {
              name: "event_names",
              label: "Filter by Event Names",
              type: "multi-select",
              placeholder: "Select event names to filter (optional)",
              options: eventNames.map((name) => ({ label: name, value: name })),
            },
          ],
        });

        metrics.push({
          label: `${label} - Monthly Sum`,
          value: `fact_${fact.id}_monthly`,
          payloads: [
            {
              name: "event_names",
              label: "Filter by Event Names",
              type: "multi-select",
              placeholder: "Select event names to filter (optional)",
              options: eventNames.map((name) => ({ label: name, value: name })),
            },
          ],
        });

        metrics.push({
          label: `${label} - Yearly Sum`,
          value: `fact_${fact.id}_yearly_sum`,
          payloads: [
            {
              name: "event_names",
              label: "Filter by Event Names",
              type: "multi-select",
              placeholder: "Select event names to filter (optional)",
              options: eventNames.map((name) => ({ label: name, value: name })),
            },
          ],
        });

        metrics.push({
          label: `${label} - Yearly Mean`,
          value: `fact_${fact.id}_yearly_mean`,
          payloads: [
            {
              name: "event_names",
              label: "Filter by Event Names",
              type: "multi-select",
              placeholder: "Select event names to filter (optional)",
              options: eventNames.map((name) => ({ label: name, value: name })),
            },
          ],
        });
      });

      return c.json(metrics);
    } catch (error) {
      console.error("Error in metrics endpoint:", error);
      return c.json({ error: "Failed to fetch metrics" }, 500);
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
        const eventNames = target.payload?.event_names;

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
