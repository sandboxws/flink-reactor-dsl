import { createElement } from "../core/jsx-runtime.js"
import type { SchemaDefinition } from "../core/schema.js"
import type {
  BaseComponentProps,
  ConstructNode,
  TypedConstructNode,
} from "../core/types.js"

// ── Validate.Reject ─────────────────────────────────────────────────

export interface ValidateRejectProps extends BaseComponentProps {
  readonly children?: ConstructNode | ConstructNode[]
}

function ValidateReject(
  props: ValidateRejectProps,
): TypedConstructNode<"Validate.Reject"> {
  const { children, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  return createElement(
    "Validate.Reject",
    { ...rest },
    ...childArray,
  ) as TypedConstructNode<"Validate.Reject">
}

// ── Validation rules ────────────────────────────────────────────────

export interface ValidationRules {
  /** Columns that must not be NULL */
  readonly notNull?: readonly string[]
  /** Column → [min, max] inclusive bounds */
  readonly range?: Record<string, readonly [number, number]>
  /** Named rule → SQL boolean expression */
  readonly expression?: Record<string, string>
}

// ── Validate ────────────────────────────────────────────────────────

export interface ValidateProps extends BaseComponentProps {
  /** Validation rules — notNull, range, and/or expression */
  readonly rules: ValidationRules
  /** Schema of valid output */
  readonly outputSchema?: SchemaDefinition
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * Validate: declarative data quality with reject routing.
 *
 * Valid records pass through downstream. Invalid records are routed
 * to a Validate.Reject sink with a `_validation_error` column
 * explaining which rule failed, and a `_validated_at` timestamp.
 *
 * Must contain exactly one `Validate.Reject` child (the reject
 * destination) plus one upstream source/transform child.
 *
 * In codegen this maps to an EXECUTE STATEMENT SET with two INSERTs:
 * one for valid records (WHERE all rules pass) and one for rejected
 * records (WHERE NOT all rules pass + CASE error column).
 */
function ValidateFactory(props: ValidateProps): ConstructNode {
  if (!props.rules) {
    throw new Error("Validate requires rules")
  }

  const { rules, children, ...rest } = props

  const hasAnyRule =
    (rules.notNull && rules.notNull.length > 0) ||
    (rules.range && Object.keys(rules.range).length > 0) ||
    (rules.expression && Object.keys(rules.expression).length > 0)

  if (!hasAnyRule) {
    throw new Error("Validate requires at least one validation rule")
  }

  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  const hasReject = childArray.some((c) => c.component === "Validate.Reject")

  if (!hasReject) {
    throw new Error("Validate requires a Validate.Reject child")
  }

  return createElement("Validate", { rules, ...rest }, ...childArray)
}

/**
 * Validate component with Reject sub-component.
 *
 * Usage:
 * ```tsx
 * <Validate
 *   rules={{
 *     notNull: ['order_id', 'user_id'],
 *     range: { amount: [0, 1000000] },
 *     expression: { valid_email: "email LIKE '%@%.%'" },
 *   }}
 * >
 *   <Validate.Reject>
 *     <KafkaSink topic="invalid-orders" />
 *   </Validate.Reject>
 *   <KafkaSource topic="raw-orders" schema={OrderSchema} />
 * </Validate>
 * ```
 */
export const Validate: typeof ValidateFactory & {
  Reject: typeof ValidateReject
} = Object.assign(ValidateFactory, {
  Reject: ValidateReject,
})
