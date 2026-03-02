import { createElement } from "../core/jsx-runtime.js"
import type { SchemaDefinition } from "../core/schema.js"
import type {
  BaseComponentProps,
  ConstructNode,
  TypedConstructNode,
} from "../core/types.js"

// ── SideOutput.Sink ─────────────────────────────────────────────────

export interface SideOutputSinkProps extends BaseComponentProps {
  readonly children?: ConstructNode | ConstructNode[]
}

function SideOutputSink(
  props: SideOutputSinkProps,
): TypedConstructNode<"SideOutput.Sink"> {
  const { children, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  return createElement(
    "SideOutput.Sink",
    { ...rest },
    ...childArray,
  ) as TypedConstructNode<"SideOutput.Sink">
}

// ── SideOutput ──────────────────────────────────────────────────────

export interface SideOutputProps extends BaseComponentProps {
  /** SQL predicate — matching records go to side sink */
  readonly condition: string
  /** Label injected as `_side_tag` column in side output */
  readonly tag?: string
  /** Schema for side output (defaults to input + metadata) */
  readonly outputSchema?: SchemaDefinition
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * SideOutput: mid-pipeline tap that siphons matching records to a side
 * sink while the main stream continues downstream.
 *
 * Must contain exactly one `SideOutput.Sink` child (the side destination)
 * plus one upstream source/transform child.
 *
 * Unlike Route (which is terminal fan-out), SideOutput is mid-pipeline:
 * the main stream keeps flowing through subsequent transforms with
 * non-matching records, while matching records are tapped to the side.
 *
 * In codegen this maps to an EXECUTE STATEMENT SET with two INSERTs:
 * one for the main path (WHERE NOT condition) and one for the side
 * path (WHERE condition + metadata columns).
 */
function SideOutputFactory(props: SideOutputProps): ConstructNode {
  if (!props.condition) {
    throw new Error("SideOutput requires a condition")
  }

  const { children, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  const hasSideSink = childArray.some((c) => c.component === "SideOutput.Sink")

  if (!hasSideSink) {
    throw new Error("SideOutput requires a SideOutput.Sink child")
  }

  return createElement("SideOutput", { ...rest }, ...childArray)
}

/**
 * SideOutput component with Sink sub-component.
 *
 * Usage:
 * ```tsx
 * <SideOutput condition="amount < 0 OR user_id IS NULL" tag="invalid-order">
 *   <SideOutput.Sink>
 *     <KafkaSink topic="order-errors" />
 *   </SideOutput.Sink>
 *   <KafkaSource topic="orders" schema={OrderSchema} />
 * </SideOutput>
 * ```
 */
export const SideOutput: typeof SideOutputFactory & {
  Sink: typeof SideOutputSink
} = Object.assign(SideOutputFactory, {
  Sink: SideOutputSink,
})
