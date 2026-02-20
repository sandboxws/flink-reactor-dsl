import type { BaseComponentProps, ConstructNode, TapConfig } from '../core/types.js';
import { createElement } from '../core/jsx-runtime.js';

// ── TumbleWindow ────────────────────────────────────────────────────

export interface TumbleWindowProps extends BaseComponentProps {
  /** Window size duration (e.g. "1 hour", "5 minutes") */
  readonly size: string;
  /** Time attribute column for the window */
  readonly on: string;
  /** Enable operator tailing for this window */
  readonly tap?: boolean | TapConfig;
  readonly children?: ConstructNode | ConstructNode[];
}

/**
 * Tumbling window: fixed-size, non-overlapping time windows.
 *
 * Wraps Aggregate children for windowed aggregation.
 * Maps to TUMBLE TVF in Flink SQL.
 * Output includes window_start and window_end metadata columns.
 */
export function TumbleWindow(props: TumbleWindowProps): ConstructNode {
  const { children, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  return createElement('TumbleWindow', { ...rest }, ...childArray);
}

// ── SlideWindow ─────────────────────────────────────────────────────

export interface SlideWindowProps extends BaseComponentProps {
  /** Window size duration (e.g. "1 hour") */
  readonly size: string;
  /** Slide interval (e.g. "15 minutes") */
  readonly slide: string;
  /** Time attribute column for the window */
  readonly on: string;
  /** Enable operator tailing for this window */
  readonly tap?: boolean | TapConfig;
  readonly children?: ConstructNode | ConstructNode[];
}

/**
 * Sliding (hopping) window: overlapping time windows that advance
 * by the slide interval.
 *
 * Maps to HOP TVF in Flink SQL.
 */
export function SlideWindow(props: SlideWindowProps): ConstructNode {
  const { children, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  return createElement('SlideWindow', { ...rest }, ...childArray);
}

// ── SessionWindow ───────────────────────────────────────────────────

export interface SessionWindowProps extends BaseComponentProps {
  /** Inactivity gap duration (e.g. "30 minutes") */
  readonly gap: string;
  /** Time attribute column for the window */
  readonly on: string;
  /** Enable operator tailing for this window */
  readonly tap?: boolean | TapConfig;
  readonly children?: ConstructNode | ConstructNode[];
}

/**
 * Session window: activity-based windows that close after a gap
 * of inactivity.
 *
 * Maps to SESSION TVF in Flink SQL.
 */
export function SessionWindow(props: SessionWindowProps): ConstructNode {
  const { children, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  return createElement('SessionWindow', { ...rest }, ...childArray);
}
