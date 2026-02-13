import type { FlinkType, BaseComponentProps, ConstructNode } from '../core/types.js';
import { createElement } from '../core/jsx-runtime.js';

// ── Rename ─────────────────────────────────────────────────────────

export interface RenameProps extends BaseComponentProps {
  /** Record mapping current field names to new field names: { currentName: 'newName' } */
  readonly columns: Record<string, string>;
  readonly children?: ConstructNode | ConstructNode[];
}

/**
 * Rename: renames one or more fields in the upstream schema.
 * Generates a SELECT with aliased renamed fields and all other fields passed through.
 */
export function Rename(props: RenameProps): ConstructNode {
  const { children, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  return createElement('Rename', { ...rest }, ...childArray);
}

// ── Drop ───────────────────────────────────────────────────────────

export interface DropProps extends BaseComponentProps {
  /** Field names to exclude from the output */
  readonly columns: readonly string[];
  readonly children?: ConstructNode | ConstructNode[];
}

/**
 * Drop: removes one or more fields from the upstream schema.
 * Generates a SELECT containing only the non-dropped fields.
 */
export function Drop(props: DropProps): ConstructNode {
  const { children, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  return createElement('Drop', { ...rest }, ...childArray);
}

// ── Cast ───────────────────────────────────────────────────────────

export interface CastProps extends BaseComponentProps {
  /** Record mapping field names to target Flink SQL types: { fieldName: 'BIGINT' } */
  readonly columns: Record<string, FlinkType>;
  /** Use TRY_CAST instead of CAST (default: false) */
  readonly safe?: boolean;
  readonly children?: ConstructNode | ConstructNode[];
}

/**
 * Cast: changes the type of one or more fields.
 * Generates CAST(field AS type) or TRY_CAST(field AS type) for targeted fields.
 */
export function Cast(props: CastProps): ConstructNode {
  const { children, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  return createElement('Cast', { ...rest }, ...childArray);
}

// ── Coalesce ───────────────────────────────────────────────────────

export interface CoalesceProps extends BaseComponentProps {
  /** Record mapping field names to default SQL expressions: { fieldName: 'defaultExpr' } */
  readonly columns: Record<string, string>;
  readonly children?: ConstructNode | ConstructNode[];
}

/**
 * Coalesce: provides default values for nullable fields.
 * Generates COALESCE(field, default) for targeted fields.
 */
export function Coalesce(props: CoalesceProps): ConstructNode {
  const { children, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  return createElement('Coalesce', { ...rest }, ...childArray);
}

// ── AddField ───────────────────────────────────────────────────────

export interface AddFieldProps extends BaseComponentProps {
  /** Record mapping new field names to SQL expressions: { newFieldName: 'sqlExpr' } */
  readonly columns: Record<string, string>;
  /** Optional type hints for the new fields: { newFieldName: 'BIGINT' } */
  readonly types?: Record<string, FlinkType>;
  readonly children?: ConstructNode | ConstructNode[];
}

/**
 * AddField: appends one or more computed fields to the upstream schema.
 * Generates SELECT *, expr AS alias for each new field.
 */
export function AddField(props: AddFieldProps): ConstructNode {
  const { children, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  return createElement('AddField', { ...rest }, ...childArray);
}
