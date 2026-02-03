import type { BaseComponentProps, ConstructNode } from '../core/types.js';
import { createElement } from '../core/jsx-runtime.js';

// ── Regular Join ────────────────────────────────────────────────────

export type JoinType = 'inner' | 'left' | 'right' | 'full' | 'anti' | 'semi';

export interface JoinHints {
  readonly broadcast?: 'left' | 'right';
}

export interface JoinProps extends BaseComponentProps {
  /** Left input stream (construct node) */
  readonly left: ConstructNode;
  /** Right input stream (construct node) */
  readonly right: ConstructNode;
  /** SQL join condition (e.g. "a.user_id = b.user_id") */
  readonly on: string;
  /** Join type (default: 'inner') */
  readonly type?: JoinType;
  /** Query hints for join optimization */
  readonly hints?: JoinHints;
  /** State TTL for join state expiry (e.g. '1h', '30min') */
  readonly stateTtl?: string;
  readonly children?: ConstructNode | ConstructNode[];
}

/**
 * Regular stream-to-stream join.
 *
 * Both `left` and `right` inputs are stored as props and also
 * attached as children so `SynthContext.buildFromTree()` creates
 * the correct DAG edges from both inputs into the join node.
 */
export function Join(props: JoinProps): ConstructNode {
  if (!props.left || !props.right) {
    throw new Error('Join requires both left and right inputs');
  }

  const { children, left, right, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  // Attach left and right as children for DAG edge creation,
  // followed by any downstream children
  return createElement(
    'Join',
    { ...rest, left: left.id, right: right.id },
    left,
    right,
    ...childArray,
  );
}

// ── Temporal Join ───────────────────────────────────────────────────

export interface TemporalJoinProps extends BaseComponentProps {
  /** The driving stream */
  readonly stream: ConstructNode;
  /** The versioned table stream */
  readonly temporal: ConstructNode;
  /** SQL join condition */
  readonly on: string;
  /** Time attribute column for FOR SYSTEM_TIME AS OF */
  readonly asOf: string;
  readonly children?: ConstructNode | ConstructNode[];
}

/**
 * Temporal join: joins a stream against a versioned table
 * using point-in-time lookup (FOR SYSTEM_TIME AS OF).
 */
export function TemporalJoin(props: TemporalJoinProps): ConstructNode {
  const { children, stream, temporal, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  return createElement(
    'TemporalJoin',
    { ...rest, stream: stream.id, temporal: temporal.id },
    stream,
    temporal,
    ...childArray,
  );
}

// ── Lookup Join ─────────────────────────────────────────────────────

export interface LookupAsyncConfig {
  readonly enabled: boolean;
  readonly capacity?: number;
  readonly timeout?: string;
}

export interface LookupCacheConfig {
  readonly type: 'lru';
  readonly maxRows: number;
  readonly ttl: string;
}

export interface LookupJoinProps extends BaseComponentProps {
  /** The driving input stream */
  readonly input: ConstructNode;
  /** Dimension table name */
  readonly table: string;
  /** JDBC connection URL for the dimension table */
  readonly url: string;
  /** SQL join condition */
  readonly on: string;
  /** Output field mapping */
  readonly select?: Record<string, string>;
  /** Async lookup configuration */
  readonly async?: LookupAsyncConfig;
  /** Lookup cache configuration */
  readonly cache?: LookupCacheConfig;
  readonly children?: ConstructNode | ConstructNode[];
}

/**
 * Lookup join: enriches a stream from an external dimension table.
 *
 * Auto-injects a `proc_time` processing-time metadata column
 * for the FOR SYSTEM_TIME AS OF proc_time clause.
 */
export function LookupJoin(props: LookupJoinProps): ConstructNode {
  const { children, input, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  return createElement(
    'LookupJoin',
    { ...rest, input: input.id, procTime: true },
    input,
    ...childArray,
  );
}

// ── Interval Join ───────────────────────────────────────────────────

export interface IntervalBounds {
  /** Lower bound expression (e.g. "order_time") */
  readonly from: string;
  /** Upper bound expression (e.g. "order_time + INTERVAL '7' DAY") */
  readonly to: string;
}

export interface IntervalJoinProps extends BaseComponentProps {
  /** Left input stream */
  readonly left: ConstructNode;
  /** Right input stream */
  readonly right: ConstructNode;
  /** SQL join condition */
  readonly on: string;
  /** Time interval bounds for the join window */
  readonly interval: IntervalBounds;
  /** Join type (default: 'inner') */
  readonly type?: 'inner' | 'left' | 'right' | 'full';
  readonly children?: ConstructNode | ConstructNode[];
}

/**
 * Interval join: time-bounded stream-to-stream join.
 *
 * Joins two streams where the time attributes fall within
 * the specified interval bounds (BETWEEN ... AND ...).
 */
export function IntervalJoin(props: IntervalJoinProps): ConstructNode {
  const { children, left, right, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  return createElement(
    'IntervalJoin',
    { ...rest, left: left.id, right: right.id },
    left,
    right,
    ...childArray,
  );
}
