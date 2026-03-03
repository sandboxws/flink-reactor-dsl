import { createElement } from "@/core/jsx-runtime.js"
import type { BaseComponentProps, ConstructNode } from "@/core/types.js"

// ── View ────────────────────────────────────────────────────────────

export interface ViewProps extends BaseComponentProps {
  /** View name (used as table reference in downstream `from` props) */
  readonly name: string
  /** Upstream query that defines the view */
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * View: named reusable intermediate query.
 *
 * Creates a Flink SQL `CREATE VIEW` from the upstream query defined
 * by its children. Downstream components can reference the view by
 * name using the `from` prop.
 *
 * This solves the fan-out problem: when multiple downstream transforms
 * need the same intermediate result, View deduplicates the upstream
 * and makes the generated SQL debuggable in Flink SQL CLI.
 *
 * Usage:
 * ```tsx
 * <View name="enriched_orders">
 *   <Map select={{ order_id: 'order_id', total: 'amount * quantity' }}>
 *     <KafkaSource topic="orders" schema={OrderSchema} />
 *   </Map>
 * </View>
 *
 * <KafkaSink topic="high-value" from="enriched_orders">
 *   <Filter condition="total > 1000" />
 * </KafkaSink>
 * ```
 */
export function View(props: ViewProps): ConstructNode {
  if (!props.name) {
    throw new Error("View requires a name")
  }

  const { children, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  return createElement("View", { ...rest }, ...childArray)
}
