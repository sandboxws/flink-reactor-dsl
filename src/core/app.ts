import type { ConstructNode, FlinkMajorVersion } from './types.js';
import type { InfraConfig, FlinkReactorConfig } from './config.js';
import type { EnvironmentConfig } from './environment.js';
import type { FlinkReactorPlugin } from './plugin.js';
import type { ResolvedConfig } from './config-resolver.js';
import { generateSql, type GenerateSqlResult } from '../codegen/sql-generator.js';
import { generateCrd, type FlinkDeploymentCrd, type CrdGeneratorOptions } from '../codegen/crd-generator.js';
import { resolveEnvironment } from './environment.js';
import { toInfraConfigFromResolved } from './config-resolver.js';
import { resolvePlugins, EMPTY_PLUGIN_CHAIN } from './plugin-registry.js';
import { registerComponentKinds, resetComponentKinds } from './jsx-runtime.js';
import { rekindTree } from './tree-utils.js';

// ── FlinkReactorApp types ────────────────────────────────────────────

export interface FlinkReactorAppProps {
  readonly name: string;
  readonly infra?: InfraConfig;
  readonly children?: ConstructNode | ConstructNode[];
}

export interface PipelineArtifact {
  readonly name: string;
  readonly sql: GenerateSqlResult;
  readonly crd: FlinkDeploymentCrd;
}

export interface AppSynthResult {
  readonly appName: string;
  readonly pipelines: readonly PipelineArtifact[];
}

// ── Configuration cascade ────────────────────────────────────────────

/**
 * Apply the configuration cascade to a pipeline node's props.
 *
 * Priority (highest to lowest):
 *   1. Pipeline prop (set directly on the Pipeline component)
 *   2. Environment override (named pipeline override > wildcard override)
 *   3. Project config (flink-reactor.config.ts / InfraConfig)
 *   4. Built-in defaults
 */
function applyConfigCascade(
  pipelineNode: ConstructNode,
  infra?: InfraConfig,
  env?: EnvironmentConfig,
  resolvedConfig?: ResolvedConfig,
): ConstructNode {
  const pipelineName = pipelineNode.props.name as string;
  const mergedProps = { ...pipelineNode.props };

  // Layer 3: InfraConfig defaults (lowest)
  if (infra?.kafka?.bootstrapServers && mergedProps.bootstrapServers === undefined) {
    // bootstrapServers propagates to source/sink children, not the pipeline itself
  }
  if (infra?.kubernetes?.namespace && mergedProps.namespace === undefined) {
    mergedProps.namespace = infra.kubernetes.namespace;
  }

  // Layer 2: Environment overrides (prefer resolvedConfig.pipelines over legacy env)
  if (resolvedConfig?.pipelines) {
    // Apply wildcard overrides first (lower priority)
    const wildcard = resolvedConfig.pipelines['*'];
    if (wildcard) {
      for (const [key, value] of Object.entries(wildcard)) {
        if (value !== undefined && mergedProps[key] === undefined) {
          mergedProps[key] = value;
        }
      }
    }
    // Apply named pipeline overrides (higher priority)
    const named = resolvedConfig.pipelines[pipelineName];
    if (named) {
      for (const [key, value] of Object.entries(named)) {
        if (value !== undefined && mergedProps[key] === undefined) {
          mergedProps[key] = value;
        }
      }
    }
  } else if (env) {
    // Legacy path: use EnvironmentConfig
    const envOverrides = resolveEnvironment(pipelineName, env);
    for (const [key, value] of Object.entries(envOverrides)) {
      if (mergedProps[key] === undefined) {
        mergedProps[key] = value;
      }
    }
  }

  return {
    ...pipelineNode,
    props: mergedProps,
  };
}

/**
 * Propagate shared infra config (e.g., bootstrapServers) to source/sink children.
 */
function propagateInfraToChildren(
  node: ConstructNode,
  infra?: InfraConfig,
): ConstructNode {
  if (!infra?.kafka?.bootstrapServers) return node;

  const bs = infra.kafka.bootstrapServers;

  const propagate = (n: ConstructNode): ConstructNode => {
    let props = n.props;

    // Only apply bootstrapServers to Source/Sink components that accept it
    // and don't already have it set
    if (
      (n.kind === 'Source' || n.kind === 'Sink') &&
      (n.component === 'KafkaSource' || n.component === 'KafkaSink') &&
      props.bootstrapServers === undefined
    ) {
      props = { ...props, bootstrapServers: bs };
    }

    const children = n.children.map((c) => propagate(c));

    return { ...n, props, children };
  };

  return propagate(node);
}

// ── FlinkReactorApp ──────────────────────────────────────────────────

/**
 * Synthesize a FlinkReactorApp: produces separate SQL + CRD per pipeline.
 *
 * Plugin integration points (in order):
 *   1. resolvePlugins() — order and validate
 *   2. registerComponentKinds() — extend KIND_MAP
 *   3. beforeSynth hooks
 *   4. per pipeline: config cascade → infra propagation → transformTree → generateSql → generateCrd → transformCrd
 *   5. afterSynth hooks
 */
export function synthesizeApp(
  props: FlinkReactorAppProps,
  options?: {
    readonly flinkVersion?: FlinkMajorVersion;
    readonly env?: EnvironmentConfig;
    readonly config?: FlinkReactorConfig;
    readonly resolvedConfig?: ResolvedConfig;
    readonly crdOptions?: Partial<CrdGeneratorOptions>;
    readonly plugins?: readonly FlinkReactorPlugin[];
  },
): AppSynthResult {
  const childArray = props.children == null
    ? []
    : Array.isArray(props.children)
      ? props.children
      : [props.children];

  // Filter to only Pipeline nodes
  const pipelineNodes = childArray.filter((c) => c.kind === 'Pipeline');

  const flinkVersion: FlinkMajorVersion = options?.flinkVersion
    ?? options?.resolvedConfig?.flink.version
    ?? options?.config?.flink?.version
    ?? '2.0';

  // Prefer infra from resolvedConfig, then props, then legacy config
  const infra = props.infra
    ?? (options?.resolvedConfig ? toInfraConfigFromResolved(options.resolvedConfig) : undefined)
    ?? options?.config?.toInfraConfig?.();

  // ── Plugin resolution ──────────────────────────────────────────────
  // Merge plugins from options and config (options take precedence / come first)
  const allPlugins = [
    ...(options?.config?.plugins ?? []),
    ...(options?.plugins ?? []),
  ];
  const chain = allPlugins.length > 0
    ? resolvePlugins(allPlugins)
    : EMPTY_PLUGIN_CHAIN;

  // Register plugin component kinds (cleaned up after synthesis)
  if (chain.components.size > 0) {
    registerComponentKinds(chain.components);
  }

  try {
    // ── beforeSynth hooks ──────────────────────────────────────────────
    if (chain.beforeSynth.length > 0) {
      const hookCtx = {
        appName: props.name,
        flinkVersion,
        pipelines: pipelineNodes,
      };
      for (const hook of chain.beforeSynth) {
        hook!(hookCtx);
      }
    }

    // ── Per-pipeline synthesis ──────────────────────────────────────────
    const pipelines: PipelineArtifact[] = pipelineNodes.map((pipelineNode) => {
      // Apply config cascade
      let node = applyConfigCascade(pipelineNode, infra, options?.env, options?.resolvedConfig);

      // Propagate infra settings to children
      node = propagateInfraToChildren(node, infra);

      // Re-resolve node kinds for plugin-registered components
      // (needed because createElement runs before plugin registration)
      if (chain.components.size > 0) {
        node = rekindTree(node, chain.components);
      }

      // Apply plugin tree transformers (composed left-to-right)
      node = chain.transformTree(node);

      const name = node.props.name as string;

      // Generate SQL (with plugin SQL/DDL generators)
      const sql = generateSql(node, {
        flinkVersion,
        pluginSqlGenerators: chain.sqlGenerators.size > 0 ? chain.sqlGenerators : undefined,
        pluginDdlGenerators: chain.ddlGenerators.size > 0 ? chain.ddlGenerators : undefined,
      });

      // Generate CRD
      const crdOpts: CrdGeneratorOptions = {
        flinkVersion,
        ...options?.crdOptions,
      };
      let crd = generateCrd(node, crdOpts);

      // Apply plugin CRD transformers
      crd = chain.transformCrd(crd, node);

      return { name, sql, crd };
    });

    // ── afterSynth hooks ───────────────────────────────────────────────
    if (chain.afterSynth.length > 0) {
      const hookCtx = {
        appName: props.name,
        flinkVersion,
        pipelines: pipelineNodes,
        results: pipelines.map((p) => ({
          name: p.name,
          sql: p.sql.sql,
          crd: p.crd,
        })),
      };
      for (const hook of chain.afterSynth) {
        hook!(hookCtx);
      }
    }

    return {
      appName: props.name,
      pipelines,
    };
  } finally {
    // Clean up plugin component registrations to avoid leaking between runs
    if (chain.components.size > 0) {
      resetComponentKinds();
    }
  }
}
