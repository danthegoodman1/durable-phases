export type DurableLogFields = Record<string, unknown>

export type DurableLogger = {
  debug(event: string, fields?: DurableLogFields): void
  info(event: string, fields?: DurableLogFields): void
  warn(event: string, fields?: DurableLogFields): void
  error(event: string, fields?: DurableLogFields): void
}

export type DurableMetricTagValue = string | number | boolean
export type DurableMetricTags = Record<string, DurableMetricTagValue | undefined>

export type DurableMetrics = {
  counter(name: string, value?: number, tags?: DurableMetricTags): void
  histogram(name: string, value: number, tags?: DurableMetricTags): void
  gauge(name: string, value: number, tags?: DurableMetricTags): void
}

export type DurableObservability = {
  logger?: DurableLogger
  metrics?: DurableMetrics
}

type LogLevel = keyof DurableLogger
type MetricKind = keyof DurableMetrics

const forbiddenMetricTags = new Set([
  "workflowId",
  "runId",
  "activationId",
  "signalId",
  "childId",
  "childRecordId",
  "effectId",
  "attemptId",
  "idempotencyKey",
])

export function logDurable(
  observability: DurableObservability | undefined,
  level: LogLevel,
  event: string,
  fields?: DurableLogFields,
): void {
  try {
    observability?.logger?.[level](event, fields)
  } catch {
    // Observability must never affect workflow correctness.
  }
}

export function countDurable(
  observability: DurableObservability | undefined,
  name: string,
  value = 1,
  tags?: DurableMetricTags,
): void {
  metricDurable(observability, "counter", name, value, tags)
}

export function histogramDurable(
  observability: DurableObservability | undefined,
  name: string,
  value: number,
  tags?: DurableMetricTags,
): void {
  metricDurable(observability, "histogram", name, value, tags)
}

export function gaugeDurable(
  observability: DurableObservability | undefined,
  name: string,
  value: number,
  tags?: DurableMetricTags,
): void {
  metricDurable(observability, "gauge", name, value, tags)
}

export function errorFields(error: unknown): DurableLogFields {
  if (error instanceof Error) {
    return {
      name: error.name,
      message: error.message,
      stack: error.stack,
    }
  }
  return { message: String(error) }
}

function metricDurable(
  observability: DurableObservability | undefined,
  kind: MetricKind,
  name: string,
  value: number,
  tags?: DurableMetricTags,
): void {
  try {
    const cleanTags = sanitizeMetricTags(tags)
    if (kind === "counter") {
      observability?.metrics?.counter(name, value, cleanTags)
    } else if (kind === "histogram") {
      observability?.metrics?.histogram(name, value, cleanTags)
    } else {
      observability?.metrics?.gauge(name, value, cleanTags)
    }
  } catch {
    // Observability must never affect workflow correctness.
  }
}

function sanitizeMetricTags(tags: DurableMetricTags | undefined): DurableMetricTags | undefined {
  if (!tags) {
    return undefined
  }

  const clean: DurableMetricTags = {}
  for (const [key, value] of Object.entries(tags)) {
    if (forbiddenMetricTags.has(key) || value === undefined) {
      continue
    }
    clean[key] = value
  }
  return Object.keys(clean).length > 0 ? clean : undefined
}
