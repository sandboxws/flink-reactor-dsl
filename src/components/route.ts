import type { BaseComponentProps, ConstructNode, TypedConstructNode } from '../core/types.js';
import { createElement } from '../core/jsx-runtime.js';

/** Branded node types for Route's structural children */
type RouteBranchNode = TypedConstructNode<'Route.Branch'>;
type RouteDefaultNode = TypedConstructNode<'Route.Default'>;

// ── Route.Branch ────────────────────────────────────────────────────

export interface RouteBranchProps extends BaseComponentProps {
  /** SQL condition expression for this branch */
  readonly condition: string;
  readonly children?: ConstructNode | ConstructNode[];
}

function RouteBranch(props: RouteBranchProps): RouteBranchNode {
  const { children, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  return createElement('Route.Branch', { ...rest }, ...childArray) as RouteBranchNode;
}

// ── Route.Default ───────────────────────────────────────────────────

export interface RouteDefaultProps extends BaseComponentProps {
  readonly children?: ConstructNode | ConstructNode[];
}

function RouteDefault(props: RouteDefaultProps): RouteDefaultNode {
  const { children, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  return createElement('Route.Default', { ...rest }, ...childArray) as RouteDefaultNode;
}

// ── Route ───────────────────────────────────────────────────────────

export interface RouteProps extends BaseComponentProps {
  // Note: In classic JSX mode, children type is ConstructNode (via JSX.Element),
  // so compile-time constraint to Branch/Default only is not possible.
  // The branded sub-component types enable this constraint when migrating to
  // "jsx": "react-jsx" mode, and the runtime validation below catches misuse.
  readonly children?: ConstructNode | ConstructNode[];
}

/**
 * Route: conditionally splits a stream into multiple downstream paths.
 *
 * Must contain at least one Route.Branch child. Optionally includes
 * a Route.Default for rows not matching any branch condition.
 *
 * Each branch becomes a separate output path in the DAG, guarded
 * by its SQL condition. In codegen this maps to multiple
 * INSERT INTO ... SELECT ... WHERE statements.
 */
function RouteFactory(props: RouteProps): ConstructNode {
  const { children, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  const hasBranch = childArray.some(
    (c) => c.component === 'Route.Branch',
  );

  if (!hasBranch) {
    throw new Error('Route requires at least one Route.Branch child');
  }

  return createElement('Route', { ...rest }, ...childArray);
}

/**
 * Route component with Branch and Default sub-components.
 *
 * Usage:
 * ```tsx
 * <Route>
 *   <Route.Branch condition="level = 'ERROR'">
 *     <KafkaSink topic="errors" />
 *   </Route.Branch>
 *   <Route.Branch condition="level = 'WARN'">
 *     <KafkaSink topic="warnings" />
 *   </Route.Branch>
 *   <Route.Default>
 *     <KafkaSink topic="other" />
 *   </Route.Default>
 * </Route>
 * ```
 */
export const Route: typeof RouteFactory & {
  Branch: typeof RouteBranch;
  Default: typeof RouteDefault;
} = Object.assign(RouteFactory, {
  Branch: RouteBranch,
  Default: RouteDefault,
});
