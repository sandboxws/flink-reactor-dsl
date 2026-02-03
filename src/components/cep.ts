import type { BaseComponentProps, ConstructNode } from '../core/types.js';
import { createElement } from '../core/jsx-runtime.js';

// ── MatchRecognize ──────────────────────────────────────────────────

export type MatchAfterStrategy = 'MATCH_RECOGNIZED' | 'NEXT_ROW';

export interface MatchRecognizeProps extends BaseComponentProps {
  /** Input stream to match patterns against */
  readonly input: ConstructNode;
  /** Row-pattern string using pattern variables (e.g. 'A B+ C') */
  readonly pattern: string;
  /** Map of pattern variable → boolean SQL condition */
  readonly define: Record<string, string>;
  /** Map of output column name → SQL expression using pattern variables */
  readonly measures: Record<string, string>;
  /** Match strategy after a match is found */
  readonly after?: MatchAfterStrategy;
  /** Fields to partition the input by before matching */
  readonly partitionBy?: readonly string[];
  /** Field to order the input by (required for event ordering) */
  readonly orderBy?: string;
  readonly children?: ConstructNode | ConstructNode[];
}

/**
 * MatchRecognize: complex event processing via MATCH_RECOGNIZE.
 *
 * Generates a MATCH_RECOGNIZE clause with PARTITION BY, ORDER BY,
 * MEASURES, PATTERN, and DEFINE subclauses. Produces a stream
 * whose schema is defined by the `measures` expressions.
 */
export function MatchRecognize(props: MatchRecognizeProps): ConstructNode {
  const { input, children, ...rest } = props;

  if (!input) {
    throw new Error('MatchRecognize requires an input stream');
  }

  if (!props.pattern) {
    throw new Error('MatchRecognize requires a pattern');
  }

  if (!props.define || Object.keys(props.define).length === 0) {
    throw new Error('MatchRecognize requires at least one DEFINE clause');
  }

  if (!props.measures || Object.keys(props.measures).length === 0) {
    throw new Error('MatchRecognize requires at least one MEASURES expression');
  }

  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  return createElement('MatchRecognize', { ...rest, input: input.id }, input, ...childArray);
}
