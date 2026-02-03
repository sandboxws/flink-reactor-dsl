import type { ConstructNode, FlinkMajorVersion } from './types.js';
import type { InfraConfig, FlinkReactorConfig } from './config.js';
import type { EnvironmentConfig } from './environment.js';
import { generateSql, type GenerateSqlResult } from '../codegen/sql-generator.js';
import { generateCrd, type FlinkDeploymentCrd, type CrdGeneratorOptions } from '../codegen/crd-generator.js';
import { resolveEnvironment } from './environment.js';

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

  // Layer 2: Environment overrides
  if (env) {
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
 */
export function synthesizeApp(
  props: FlinkReactorAppProps,
  options?: {
    readonly flinkVersion?: FlinkMajorVersion;
    readonly env?: EnvironmentConfig;
    readonly config?: FlinkReactorConfig;
    readonly crdOptions?: Partial<CrdGeneratorOptions>;
  },
): AppSynthResult {
  const childArray = props.children == null
    ? []
    : Array.isArray(props.children)
      ? props.children
      : [props.children];

  // Filter to only Pipeline nodes
  const pipelineNodes = childArray.filter((c) => c.kind === 'Pipeline');

  const flinkVersion = options?.flinkVersion
    ?? options?.config?.flink?.version
    ?? '2.0';

  const infra = props.infra ?? options?.config?.toInfraConfig?.();

  const pipelines: PipelineArtifact[] = pipelineNodes.map((pipelineNode) => {
    // Apply config cascade
    let node = applyConfigCascade(pipelineNode, infra, options?.env);

    // Propagate infra settings to children
    node = propagateInfraToChildren(node, infra);

    const name = node.props.name as string;

    // Generate SQL
    const sql = generateSql(node, { flinkVersion });

    // Generate CRD
    const crdOpts: CrdGeneratorOptions = {
      flinkVersion,
      ...options?.crdOptions,
    };
    const crd = generateCrd(node, crdOpts);

    return { name, sql, crd };
  });

  return {
    appName: props.name,
    pipelines,
  };
}
