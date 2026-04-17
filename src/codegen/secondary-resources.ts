// ── Kubernetes secondary resources ──────────────────────────────────
// The synthesis engine's primary output is a FlinkDeployment CRD. Some
// pipelines (notably Flink CDC Pipeline Connector jobs) need additional
// k8s resources alongside it — e.g. a ConfigMap that holds the
// `pipeline.yaml` document the job mounts at runtime.
//
// This module models those sibling resources as a typed discriminated
// union so the CLI can iterate and emit one file per resource without
// knowing the concrete shape of each kind.

import { toYaml } from "./crd-generator.js"

// ── Types ───────────────────────────────────────────────────────────

export interface ConfigMapResource {
  readonly kind: "ConfigMap"
  readonly apiVersion: "v1"
  readonly metadata: {
    readonly name: string
    readonly labels?: Record<string, string>
  }
  readonly data: Record<string, string>
}

/**
 * Discriminated union of secondary Kubernetes resources emitted alongside
 * the primary FlinkDeployment CRD.
 *
 * Intentionally *not* including `Secret` — the DSL references Secrets by
 * name but never emits their cleartext contents (those are owned by the
 * user or by an external-secrets operator).
 */
export type SecondaryResource = ConfigMapResource

// ── Builders ────────────────────────────────────────────────────────

/**
 * Build a ConfigMap that holds the Flink CDC pipeline.yaml document.
 * The ConfigMap name follows the `<pipeline-name>-pipeline` convention
 * so the FlinkDeployment podTemplate can reference it deterministically.
 */
export function buildPipelineYamlConfigMap(
  pipelineName: string,
  pipelineYaml: string,
): ConfigMapResource {
  return {
    apiVersion: "v1",
    kind: "ConfigMap",
    metadata: {
      name: pipelineYamlConfigMapName(pipelineName),
    },
    data: {
      "pipeline.yaml": pipelineYaml,
    },
  }
}

/** The ConfigMap name the FlinkDeployment references for its pipeline.yaml mount. */
export function pipelineYamlConfigMapName(pipelineName: string): string {
  return `${pipelineName}-pipeline`
}

// ── Serialization ───────────────────────────────────────────────────

/** Serialize a secondary resource to a YAML document string. */
export function secondaryResourceToYaml(res: SecondaryResource): string {
  return toYaml(res)
}
