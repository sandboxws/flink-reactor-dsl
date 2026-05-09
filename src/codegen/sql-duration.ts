/**
 * Duration parsing helpers shared by SQL codegen.
 *
 * Pipelines accept durations in three shapes:
 *   1. Already-formatted Flink intervals (`INTERVAL '5' SECOND`) — passed
 *      through verbatim.
 *   2. Bare millisecond integers (`"60000"`) — common from props that store
 *      raw numbers.
 *   3. Human shorthand (`"5s"`, `"2 minutes"`, `"1 hour"`, `"1d"`) — the
 *      DSL's preferred form.
 *
 * `toInterval` produces a Flink SQL `INTERVAL` expression; `toMilliseconds`
 * normalises to millisecond integers. Both are pure and side-effect free.
 */

export function toInterval(duration: string): string {
  const lower = duration.trim().toLowerCase()

  if (lower.startsWith("interval")) return duration

  if (/^\d+$/.test(lower)) {
    const ms = parseInt(lower, 10)
    if (ms % 86400000 === 0) return `INTERVAL '${ms / 86400000}' DAY`
    if (ms % 3600000 === 0) return `INTERVAL '${ms / 3600000}' HOUR`
    if (ms % 60000 === 0) return `INTERVAL '${ms / 60000}' MINUTE`
    return `INTERVAL '${ms / 1000}' SECOND`
  }

  const match = lower.match(
    /^(\d+)\s*(s|sec|second|seconds|m|min|minute|minutes|h|hr|hour|hours|d|day|days)$/,
  )
  if (match) {
    const value = match[1]
    const unit = match[2]
    if (unit.startsWith("s")) return `INTERVAL '${value}' SECOND`
    if (unit.startsWith("m")) return `INTERVAL '${value}' MINUTE`
    if (unit.startsWith("h")) return `INTERVAL '${value}' HOUR`
    if (unit.startsWith("d")) return `INTERVAL '${value}' DAY`
  }

  return `INTERVAL '${duration}'`
}

export function toMilliseconds(duration: string): number {
  const lower = duration.trim().toLowerCase()

  if (/^\d+$/.test(lower)) return parseInt(lower, 10)

  const match = lower.match(
    /^(\d+)\s*(ms|s|sec|second|seconds|m|min|minute|minutes|h|hr|hour|hours)$/,
  )
  if (match) {
    const value = parseInt(match[1], 10)
    const unit = match[2]
    if (unit === "ms") return value
    if (unit.startsWith("s")) return value * 1000
    if (unit.startsWith("m")) return value * 60000
    if (unit.startsWith("h")) return value * 3600000
  }

  return parseInt(lower, 10)
}

/**
 * Strip changelog-encoding formats to their base serialization format.
 * upsert-kafka handles changelog via key semantics — the value format must
 * be insert-only (e.g. json, avro).
 */
export function toInsertOnlyFormat(format: string): string {
  switch (format) {
    case "debezium-json":
    case "canal-json":
      return "json"
    case "debezium-avro":
      return "avro"
    case "debezium-protobuf":
      return "protobuf"
    default:
      return format
  }
}
