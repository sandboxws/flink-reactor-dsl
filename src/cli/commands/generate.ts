import { existsSync, mkdirSync, writeFileSync } from "node:fs"
import { join } from "node:path"
import type { Command } from "commander"
import pc from "picocolors"

export function registerGenerateCommand(program: Command): void {
  const generate = program
    .command("generate")
    .alias("g")
    .description("Generate a new component")

  generate
    .command("pipeline")
    .argument("<name>", "Pipeline name")
    .option(
      "-t, --template <template>",
      "Pipeline template (blank, kafka, jdbc)",
      "blank",
    )
    .description("Generate a new pipeline")
    .action((name: string, opts: Record<string, string>) => {
      generatePipeline(name, opts.template ?? "blank")
    })

  generate
    .command("schema")
    .argument("<name>", "Schema name")
    .description("Generate a new schema file")
    .action((name: string) => {
      generateSchema(name)
    })

  generate
    .command("env")
    .argument("<name>", "Environment name")
    .description("Generate a new environment config")
    .action((name: string) => {
      generateEnv(name)
    })

  generate
    .command("pattern")
    .argument("<name>", "Pattern name")
    .description("Generate a new reusable pattern")
    .action((name: string) => {
      generatePattern(name)
    })

  generate
    .command("app")
    .argument("<name>", "App name")
    .description("Generate a new app (monorepo only)")
    .action((name: string) => {
      generateApp(name)
    })

  generate
    .command("package")
    .argument("<name>", "Package name")
    .description("Generate a new package (monorepo only)")
    .action((name: string) => {
      generatePackage(name)
    })
}

function writeIfNotExists(filePath: string, content: string): boolean {
  if (existsSync(filePath)) {
    console.error(pc.red(`Error: ${filePath} already exists.`))
    process.exitCode = 1
    return false
  }
  const dir = join(filePath, "..")
  mkdirSync(dir, { recursive: true })
  writeFileSync(filePath, content, "utf-8")
  console.log(`  ${pc.green("✓")} Created ${pc.dim(filePath)}`)
  return true
}

function isMonorepo(): boolean {
  return existsSync(join(process.cwd(), "pnpm-workspace.yaml"))
}

function toPascalCase(name: string): string {
  return name
    .split(/[-_]/)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join("")
}

export function generatePipeline(name: string, template: string): void {
  const dir = join(process.cwd(), "pipelines", name)
  const filePath = join(dir, "index.tsx")
  const testPath = join(process.cwd(), "tests", "pipelines", `${name}.test.ts`)

  let content: string

  switch (template) {
    case "kafka":
      content = `import { Pipeline, KafkaSource, KafkaSink } from 'flink-reactor';

export default (
  <Pipeline name="${name}">
    <KafkaSource
      topic="input-topic"
      schema={/* TODO: import schema */}
      bootstrapServers="localhost:9092"
      consumerGroup="${name}"
    />
    <KafkaSink
      topic="output-topic"
      bootstrapServers="localhost:9092"
    />
  </Pipeline>
);
`
      break

    case "jdbc":
      content = `import { Pipeline, KafkaSource, JdbcSink } from 'flink-reactor';

export default (
  <Pipeline name="${name}">
    <KafkaSource
      topic="input-topic"
      schema={/* TODO: import schema */}
      bootstrapServers="localhost:9092"
      consumerGroup="${name}"
    />
    <JdbcSink
      table="${name.replace(/-/g, "_")}"
      url="jdbc:postgresql://localhost:5432/mydb"
    />
  </Pipeline>
);
`
      break

    default: // blank
      content = `import { Pipeline } from 'flink-reactor';

export default (
  <Pipeline name="${name}">
    {/* Add sources, transforms, and sinks here */}
  </Pipeline>
);
`
  }

  const testContent = `import { describe, it, expect } from 'vitest';
// import { synth } from 'flink-reactor/testing';

describe('${name} pipeline', () => {
  it.todo('synthesizes valid Flink SQL');
});
`

  writeIfNotExists(filePath, content)
  writeIfNotExists(testPath, testContent)
}

export function generateSchema(name: string): void {
  const filePath = join(process.cwd(), "schemas", `${name}.ts`)
  const pascalName = toPascalCase(name)

  const content = `import { Schema, Field } from 'flink-reactor';

export const ${pascalName}Schema = Schema({
  fields: {
    id: Field.BIGINT(),
    // TODO: add fields
    createdAt: Field.TIMESTAMP(3),
  },
});
`

  writeIfNotExists(filePath, content)
}

export function generateEnv(name: string): void {
  const filePath = join(process.cwd(), "env", `${name}.ts`)

  const content = `import { defineEnvironment } from 'flink-reactor';

export default defineEnvironment({
  name: '${name}',
  // Override pipeline defaults for ${name} environment
});
`

  writeIfNotExists(filePath, content)
}

export function generatePattern(name: string): void {
  const filePath = join(process.cwd(), "patterns", `${name}.ts`)
  const pascalName = toPascalCase(name)

  const content = `/**
 * ${pascalName} pattern
 * A reusable pipeline pattern.
 */
export function ${pascalName}(props: { /* TODO: define props */ }) {
  // TODO: implement pattern
}
`

  writeIfNotExists(filePath, content)
}

export function generateApp(name: string): void {
  if (!isMonorepo()) {
    console.error(
      pc.red('Error: "generate app" is only available in monorepo projects.'),
    )
    console.log(
      pc.dim(
        '  Hint: Run "flink-reactor new --template monorepo" to create a monorepo project.',
      ),
    )
    process.exitCode = 1
    return
  }

  const appDir = join(process.cwd(), "apps", name)

  const pkg = {
    name: `@${getWorkspaceName()}/${name}`,
    version: "0.1.0",
    private: true,
    type: "module",
    dependencies: {
      "flink-reactor": "^0.1.0",
    },
    devDependencies: {
      typescript: "^5.7.0",
      vitest: "^3.0.0",
    },
  }

  writeIfNotExists(
    join(appDir, "package.json"),
    `${JSON.stringify(pkg, null, 2)}\n`,
  )
  writeIfNotExists(
    join(appDir, "flink-reactor.config.ts"),
    `import { defineConfig } from 'flink-reactor';

export default defineConfig({});
`,
  )
  writeIfNotExists(
    join(appDir, "env", "dev.ts"),
    `import { defineEnvironment } from 'flink-reactor';

export default defineEnvironment({
  name: 'dev',
});
`,
  )
  writeIfNotExists(join(appDir, "pipelines", ".gitkeep"), "")
  writeIfNotExists(join(appDir, "tests", ".gitkeep"), "")
}

export function generatePackage(name: string): void {
  if (!isMonorepo()) {
    console.error(
      pc.red(
        'Error: "generate package" is only available in monorepo projects.',
      ),
    )
    console.log(
      pc.dim(
        '  Hint: Run "flink-reactor new --template monorepo" to create a monorepo project.',
      ),
    )
    process.exitCode = 1
    return
  }

  const pkgDir = join(process.cwd(), "packages", name)

  const pkg = {
    name: `@${getWorkspaceName()}/${name}`,
    version: "0.1.0",
    private: true,
    type: "module",
    main: "index.ts",
    dependencies: {
      "flink-reactor": "^0.1.0",
    },
  }

  writeIfNotExists(
    join(pkgDir, "package.json"),
    `${JSON.stringify(pkg, null, 2)}\n`,
  )
  writeIfNotExists(
    join(pkgDir, "index.ts"),
    `// Export ${name} components here\n`,
  )
}

function getWorkspaceName(): string {
  try {
    const pkgPath = join(process.cwd(), "package.json")
    if (existsSync(pkgPath)) {
      const pkg = JSON.parse(require("node:fs").readFileSync(pkgPath, "utf-8"))
      return pkg.name ?? "workspace"
    }
  } catch {
    // ignore
  }
  return "workspace"
}
