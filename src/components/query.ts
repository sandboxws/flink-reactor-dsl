import type { BaseComponentProps, ConstructNode } from '../core/types.js';
import type { SchemaDefinition } from '../core/schema.js';
import { createElement } from '../core/jsx-runtime.js';

// ── Types ────────────────────────────────────────────────────────────

/** Window specification for PARTITION BY / ORDER BY */
export interface WindowSpec {
  readonly partitionBy?: readonly string[];
  readonly orderBy?: Record<string, 'ASC' | 'DESC'>;
}

/** Structured window function expression */
export interface WindowFunctionExpr {
  /** Function name (LAG, LEAD, ROW_NUMBER, RANK, etc.) */
  readonly func: string;
  /** Function arguments */
  readonly args?: readonly (string | number)[];
  /** Reference to a named window */
  readonly window?: string;
  /** Inline window specification */
  readonly over?: WindowSpec;
}

/** A column expression: either a plain SQL string or a window function object */
export type ColumnExpr = string | WindowFunctionExpr;

// ── Clause types ─────────────────────────────────────────────────────

const QUERY_CLAUSE_TYPES = new Set([
  'Query.Select',
  'Query.Where',
  'Query.GroupBy',
  'Query.Having',
  'Query.OrderBy',
]);

export { QUERY_CLAUSE_TYPES };

// ── Query.Select ─────────────────────────────────────────────────────

export interface QuerySelectProps extends BaseComponentProps {
  /** Output alias -> expression or WindowFunctionExpr */
  readonly columns: Record<string, ColumnExpr>;
  /** Named window definitions */
  readonly windows?: Record<string, WindowSpec>;
  readonly children?: ConstructNode | ConstructNode[];
}

function QuerySelect(props: QuerySelectProps): ConstructNode {
  const { children, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  return createElement('Query.Select', { ...rest }, ...childArray);
}

// ── Query.Where ──────────────────────────────────────────────────────

export interface QueryWhereProps extends BaseComponentProps {
  /** SQL WHERE condition */
  readonly condition: string;
  readonly children?: ConstructNode | ConstructNode[];
}

function QueryWhere(props: QueryWhereProps): ConstructNode {
  const { children, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  return createElement('Query.Where', { ...rest }, ...childArray);
}

// ── Query.GroupBy ────────────────────────────────────────────────────

export interface QueryGroupByProps extends BaseComponentProps {
  /** Columns to group by */
  readonly columns: readonly string[];
  readonly children?: ConstructNode | ConstructNode[];
}

function QueryGroupBy(props: QueryGroupByProps): ConstructNode {
  const { children, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  return createElement('Query.GroupBy', { ...rest }, ...childArray);
}

// ── Query.Having ─────────────────────────────────────────────────────

export interface QueryHavingProps extends BaseComponentProps {
  /** SQL HAVING condition */
  readonly condition: string;
  readonly children?: ConstructNode | ConstructNode[];
}

function QueryHaving(props: QueryHavingProps): ConstructNode {
  const { children, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  return createElement('Query.Having', { ...rest }, ...childArray);
}

// ── Query.OrderBy ────────────────────────────────────────────────────

export interface QueryOrderByProps extends BaseComponentProps {
  /** Column -> sort direction */
  readonly columns: Record<string, 'ASC' | 'DESC'>;
  readonly children?: ConstructNode | ConstructNode[];
}

function QueryOrderBy(props: QueryOrderByProps): ConstructNode {
  const { children, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  return createElement('Query.OrderBy', { ...rest }, ...childArray);
}

// ── Query ────────────────────────────────────────────────────────────

export interface QueryProps extends BaseComponentProps {
  /** Schema describing the output of the query */
  readonly outputSchema: SchemaDefinition;
  readonly children?: ConstructNode | ConstructNode[];
}

function QueryFactory(props: QueryProps): ConstructNode {
  const { children, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  // Partition children into clause nodes and upstream data nodes
  const clauseChildren = childArray.filter(
    (c) => QUERY_CLAUSE_TYPES.has(c.component),
  );

  // Validation: must have exactly one Select
  const selectCount = clauseChildren.filter(
    (c) => c.component === 'Query.Select',
  ).length;
  if (selectCount === 0) {
    throw new Error('Query requires a Query.Select child');
  }
  if (selectCount > 1) {
    throw new Error('Query must have at most one Query.Select child');
  }

  // Validation: at most one of each clause type
  for (const clauseType of QUERY_CLAUSE_TYPES) {
    const count = clauseChildren.filter(
      (c) => c.component === clauseType,
    ).length;
    if (count > 1) {
      throw new Error(`Query must have at most one ${clauseType} child`);
    }
  }

  // Validation: Having requires GroupBy
  const hasHaving = clauseChildren.some(
    (c) => c.component === 'Query.Having',
  );
  const hasGroupBy = clauseChildren.some(
    (c) => c.component === 'Query.GroupBy',
  );
  if (hasHaving && !hasGroupBy) {
    throw new Error('Query.Having requires a Query.GroupBy sibling');
  }

  return createElement('Query', { ...rest }, ...childArray);
}

/**
 * Query: structured SQL escape hatch with JSX clause children.
 *
 * Provides a composable, TypeScript-first alternative to RawSQL
 * for window functions, HAVING clauses, and custom SELECT projections.
 *
 * Usage:
 * ```tsx
 * <Query outputSchema={OutputSchema}>
 *   <Query.Select columns={{
 *     user_id: 'user_id',
 *     total: 'SUM(amount)',
 *   }} />
 *   <Query.GroupBy columns={['user_id']} />
 *   <Query.Having condition="SUM(amount) > 1000" />
 *   <KafkaSource ... />
 * </Query>
 * ```
 */
export const Query: typeof QueryFactory & {
  Select: typeof QuerySelect;
  Where: typeof QueryWhere;
  GroupBy: typeof QueryGroupBy;
  Having: typeof QueryHaving;
  OrderBy: typeof QueryOrderBy;
} = Object.assign(QueryFactory, {
  Select: QuerySelect,
  Where: QueryWhere,
  GroupBy: QueryGroupBy,
  Having: QueryHaving,
  OrderBy: QueryOrderBy,
});
