import type { ConstructNode, NodeKind } from './types.js';
import type { FlinkDeploymentCrd } from '../codegen/crd-generator.js';
import type { ValidationDiagnostic } from './synth-context.js';

// ── Synthesis hook context ──────────────────────────────────────────

/** Context provided to beforeSynth / afterSynth lifecycle hooks */
export interface SynthHookContext {
  /** The application name from FlinkReactorApp */
  readonly appName: string;
  /** Resolved Flink version for this synthesis run */
  readonly flinkVersion: string;
  /** Pipeline construct trees (before transforms for beforeSynth, after for afterSynth) */
  readonly pipelines: readonly ConstructNode[];
}

/** Result of a single pipeline synthesis (passed to afterSynth) */
export interface PipelineSynthHookResult {
  readonly name: string;
  readonly sql: string;
  readonly crd: FlinkDeploymentCrd;
}

/** Extended context for afterSynth that includes synthesis results */
export interface AfterSynthHookContext extends SynthHookContext {
  readonly results: readonly PipelineSynthHookResult[];
}

// ── Plugin SQL/DDL generator signatures ─────────────────────────────

/**
 * Custom SQL query builder for a component type.
 * Receives the node and a lookup index of all nodes in the tree.
 * Must return a SQL SELECT expression string.
 */
export type PluginSqlGenerator = (
  node: ConstructNode,
  nodeIndex: ReadonlyMap<string, ConstructNode>,
) => string;

/**
 * Custom DDL generator for a component type.
 * Returns a DDL statement string, or null to skip DDL for this node.
 */
export type PluginDdlGenerator = (
  node: ConstructNode,
) => string | null;

// ── Plugin validator signature ──────────────────────────────────────

/**
 * Custom validation function added by a plugin.
 * Receives the construct tree root and existing diagnostics from
 * built-in validations. Returns additional diagnostics.
 */
export type PluginValidator = (
  tree: ConstructNode,
  existingDiagnostics: readonly ValidationDiagnostic[],
) => ValidationDiagnostic[];

// ── FlinkReactorPlugin interface ────────────────────────────────────

/**
 * The plugin interface for FlinkReactor.
 *
 * Plugins are plain objects — they can be local functions or npm packages.
 * All fields except `name` are optional; implement only the layers you need.
 *
 * ## Layers
 *
 * 1. **Component Registration** — register new component kinds (`components`)
 * 2. **Tree Transformers** — modify construct tree before synthesis (`transformTree`)
 * 3. **Synthesis Extensions** — custom SQL/DDL for new component types (`sqlGenerators`, `ddlGenerators`)
 * 4. **CRD Post-processing** — modify generated CRD (`transformCrd`)
 * 5. **Validation** — add custom validation rules (`validate`)
 * 6. **Lifecycle Hooks** — run code before/after synthesis (`beforeSynth`, `afterSynth`)
 *
 * ## Determinism Contract
 *
 * All transformer/generator functions MUST be pure:
 * - No `Date.now()`, `Math.random()`, or I/O
 * - Same input must always produce the same output
 */
export interface FlinkReactorPlugin {
  /** Unique plugin name (used for ordering and conflict detection) */
  readonly name: string;
  /** Semver version string (informational) */
  readonly version?: string;
  /**
   * Ordering constraints relative to other plugins.
   * `before`: this plugin runs before the named plugins.
   * `after`: this plugin runs after the named plugins.
   */
  readonly ordering?: {
    readonly before?: readonly string[];
    readonly after?: readonly string[];
  };

  // ── Layer 1: Component Registration ──────────────────────────────

  /** Register new component kinds (component name → NodeKind) */
  readonly components?: ReadonlyMap<string, NodeKind>;

  // ── Layer 2: Tree Transformers ───────────────────────────────────

  /**
   * Transform the construct tree before synthesis.
   * Must return a new tree (or the same reference if unchanged).
   * Composed left-to-right: plugin 1's output → plugin 2's input.
   */
  readonly transformTree?: (tree: ConstructNode) => ConstructNode;

  // ── Layer 3: Synthesis Extensions ────────────────────────────────

  /** Custom SQL query builders keyed by component name */
  readonly sqlGenerators?: ReadonlyMap<string, PluginSqlGenerator>;
  /** Custom DDL generators keyed by component name */
  readonly ddlGenerators?: ReadonlyMap<string, PluginDdlGenerator>;

  // ── Layer 4: CRD Post-processing ─────────────────────────────────

  /**
   * Transform the generated CRD after generation.
   * Receives the CRD and the pipeline node that produced it.
   * Must return the (possibly modified) CRD.
   */
  readonly transformCrd?: (
    crd: FlinkDeploymentCrd,
    pipelineNode: ConstructNode,
  ) => FlinkDeploymentCrd;

  // ── Layer 5: Validation ──────────────────────────────────────────

  /**
   * Add custom validation rules.
   * Receives the tree root and diagnostics from prior validators.
   * Returns additional diagnostics to append.
   */
  readonly validate?: PluginValidator;

  // ── Layer 6: Lifecycle Hooks ─────────────────────────────────────

  /** Called before synthesis begins (after plugin resolution) */
  readonly beforeSynth?: (context: SynthHookContext) => void;
  /** Called after all pipelines have been synthesized */
  readonly afterSynth?: (context: AfterSynthHookContext) => void;
}
