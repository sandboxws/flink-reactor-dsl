import { describe, expect, it } from "vitest"
import type { ProjectContext } from "@/cli/discovery.js"
import { defineConfig } from "@/core/config.js"
import { resolveConfig } from "@/core/config-resolver.js"
import { selectAdapter } from "../select.js"

function makeCtx(
  envName: string,
  envEntry: Record<string, unknown>,
): ProjectContext {
  const config = defineConfig({
    environments: { [envName]: envEntry },
  })
  return {
    projectDir: "/tmp/fake",
    config,
    env: null,
    resolvedConfig: resolveConfig(config, envName),
    pipelines: [],
  }
}

describe("selectAdapter()", () => {
  it("picks the env's runtime when no override is given", () => {
    const ctx = makeCtx("development", {
      runtime: "docker",
      supportedRuntimes: ["docker", "minikube"],
    })
    expect(selectAdapter(ctx).name).toBe("docker")
  })

  it("honors a runtime override that is in supportedRuntimes", () => {
    const ctx = makeCtx("development", {
      runtime: "docker",
      supportedRuntimes: ["docker", "minikube"],
      kubectl: { context: "minikube" },
    })
    expect(selectAdapter(ctx, "minikube").name).toBe("minikube")
  })

  it("rejects an override not listed in supportedRuntimes", () => {
    const ctx = makeCtx("development", {
      runtime: "docker",
      supportedRuntimes: ["docker"],
    })
    expect(() => selectAdapter(ctx, "minikube")).toThrow(
      /not supported by environment 'development'/,
    )
  })

  it("rejects completely unknown runtime overrides", () => {
    const ctx = makeCtx("development", { runtime: "docker" })
    expect(() => selectAdapter(ctx, "swarm" as unknown as "docker")).toThrow(
      /Unknown runtime 'swarm'/,
    )
  })

  it("falls back to docker when there is no resolved config", () => {
    const ctx: ProjectContext = {
      projectDir: "/tmp/fake",
      config: null,
      env: null,
      resolvedConfig: null,
      pipelines: [],
    }
    expect(selectAdapter(ctx).name).toBe("docker")
  })
})
