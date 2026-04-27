import { execSync } from "node:child_process"
import { existsSync, mkdirSync, writeFileSync } from "node:fs"
import { join, resolve } from "node:path"
import * as clack from "@clack/prompts"
import { type Command, Option } from "commander"
import { Effect } from "effect"
import Handlebars from "handlebars"
import pc from "picocolors"
import { runCommand } from "@/cli/effect-runner.js"
import { getBankingTemplates } from "@/cli/templates/banking.js"
import { getCdcLakehouseTemplates } from "@/cli/templates/cdc-lakehouse.js"
import { getEcommerceTemplates } from "@/cli/templates/ecommerce.js"
import { getGroceryDeliveryTemplates } from "@/cli/templates/grocery-delivery.js"
import { getIotFactoryTemplates } from "@/cli/templates/iot-factory.js"
import { getLakehouseAnalyticsTemplates } from "@/cli/templates/lakehouse-analytics.js"
import { getLakehouseIngestionTemplates } from "@/cli/templates/lakehouse-ingestion.js"
import { getMinimalTemplates } from "@/cli/templates/minimal.js"
import { getMonorepoTemplates } from "@/cli/templates/monorepo.js"
import { getRealtimeAnalyticsTemplates } from "@/cli/templates/realtime-analytics.js"
import { getRideSharingTemplates } from "@/cli/templates/ride-sharing.js"
import { getStarterTemplates } from "@/cli/templates/starter.js"
import { getStockBasicsTemplates } from "@/cli/templates/stock-basics.js"
import { getStockDsEasyTemplates } from "@/cli/templates/stock-ds-easy.js"
import { getStockDsModerateTemplates } from "@/cli/templates/stock-ds-moderate.js"
import { getStockTemporalTopnTemplates } from "@/cli/templates/stock-temporal-topn.js"
import { CliError } from "@/core/errors.js"

export type TemplateName =
  | "starter"
  | "minimal"
  | "cdc-lakehouse"
  | "realtime-analytics"
  | "monorepo"
  | "ecommerce"
  | "ride-sharing"
  | "grocery-delivery"
  | "banking"
  | "iot-factory"
  | "lakehouse-ingestion"
  | "lakehouse-analytics"
  | "stock-basics"
  | "stock-ds-easy"
  | "stock-ds-moderate"
  | "stock-temporal-topn"
export type PackageManager = "pnpm" | "npm" | "yarn"
export type FlinkVersion = "1.20" | "2.0" | "2.1" | "2.2"

export interface ScaffoldOptions {
  projectName: string
  template: TemplateName
  pm: PackageManager
  flinkVersion: FlinkVersion
  gitInit: boolean
  installDeps: boolean
  registry?: string
}

export type RegistryChoice = "npm" | "verdaccio"

const REGISTRY_URLS: Record<RegistryChoice, string | undefined> = {
  npm: undefined,
  verdaccio: "http://localhost:4873",
}

export interface TemplateFile {
  path: string
  content: string
}

type TemplateFactory = (opts: ScaffoldOptions) => TemplateFile[]

const TEMPLATE_FACTORIES: Record<TemplateName, TemplateFactory> = {
  starter: getStarterTemplates,
  minimal: getMinimalTemplates,
  "cdc-lakehouse": getCdcLakehouseTemplates,
  "realtime-analytics": getRealtimeAnalyticsTemplates,
  monorepo: getMonorepoTemplates,
  ecommerce: getEcommerceTemplates,
  "ride-sharing": getRideSharingTemplates,
  "grocery-delivery": getGroceryDeliveryTemplates,
  banking: getBankingTemplates,
  "iot-factory": getIotFactoryTemplates,
  "lakehouse-ingestion": getLakehouseIngestionTemplates,
  "lakehouse-analytics": getLakehouseAnalyticsTemplates,
  "stock-basics": getStockBasicsTemplates,
  "stock-ds-easy": getStockDsEasyTemplates,
  "stock-ds-moderate": getStockDsModerateTemplates,
  "stock-temporal-topn": getStockTemporalTopnTemplates,
}

const TEMPLATE_DESCRIPTIONS: Record<TemplateName, string> = {
  starter: "Kafka source → transform → Kafka sink",
  minimal: "Empty project structure",
  "cdc-lakehouse": "Debezium CDC → Iceberg lakehouse (upsert)",
  "realtime-analytics": "Kafka → windowed aggregation → JDBC",
  monorepo: "pnpm workspace with packages/ and apps/",
  ecommerce: "3-way joins, Top-N, session windows (3 pipelines + pump)",
  "ride-sharing": "CEP trip tracking, broadcast surge pricing (2 pipelines)",
  "grocery-delivery":
    "CDC inventory, dedup + Top-N store rankings (2 pipelines)",
  banking: "MATCH_RECOGNIZE fraud detection, compliance fan-out (2 pipelines)",
  "iot-factory": "Sliding window anomaly detection (1 pipeline + pump)",
  "lakehouse-ingestion":
    "Multi-topic Kafka → Iceberg raw landing (3 tables + pump)",
  "lakehouse-analytics":
    "Medallion architecture: bronze → silver → gold with Iceberg (3 pipelines + pump)",
  "stock-basics":
    "Apache Flink basics: WordCount, GettingStarted, Stream-SQL Union, Stream-Window SQL (4 pipelines)",
  "stock-ds-easy":
    "DataStream→FlinkSQL migration showcase (easy bucket): wordcount, session/tumble windows, interval join (5 pipelines)",
  "stock-ds-moderate":
    "DataStream→FlinkSQL migration showcase (moderate bucket): join DSv2, side-output routing, MATCH_RECOGNIZE state machine (4 pipelines + pump)",
  "stock-temporal-topn":
    "Apache Flink advanced: temporal/versioned joins + continuous Top-N (2 pipelines + pump)",
}

export function registerNewCommand(program: Command): void {
  const TEMPLATE_NAMES = Object.keys(TEMPLATE_FACTORIES) as TemplateName[]
  const PM_CHOICES: PackageManager[] = ["pnpm", "npm", "yarn"]
  const FLINK_VERSION_CHOICES: FlinkVersion[] = ["1.20", "2.0", "2.1", "2.2"]

  program
    .command("new")
    .argument("<project-name>", "Name of the new project")
    .addOption(
      new Option("-t, --template <template>", "Project template").choices(
        TEMPLATE_NAMES,
      ),
    )
    .addOption(new Option("--pm <pm>", "Package manager").choices(PM_CHOICES))
    .addOption(
      new Option("--flink-version <version>", "Flink version").choices(
        FLINK_VERSION_CHOICES,
      ),
    )
    .addOption(
      new Option("--registry <registry>", "npm registry").choices([
        "npm",
        "verdaccio",
      ]),
    )
    .option("-y, --yes", "Use defaults, skip prompts")
    .option("--no-git", "Skip git initialization")
    .option("--no-install", "Skip dependency installation")
    .description("Create a new FlinkReactor project")
    .action(async (projectName: string, opts: Record<string, unknown>) => {
      await runCommand(
        Effect.tryPromise({
          try: () => runNewCommand(projectName, opts),
          catch: (err) =>
            new CliError({
              reason: "invalid_args",
              message: (err as Error).message,
            }),
        }),
      )
    })
}

export async function runNewCommand(
  projectName: string,
  opts: Record<string, unknown>,
): Promise<void> {
  let projectDir: string
  try {
    projectDir = resolve(projectName)
  } catch {
    console.error(
      pc.red(
        "Error: Current directory no longer exists. Please cd to a valid directory and try again.",
      ),
    )
    process.exitCode = 1
    return
  }

  if (existsSync(projectDir)) {
    console.error(pc.red(`Error: Directory "${projectName}" already exists.`))
    process.exitCode = 1
    return
  }

  const nonInteractive = opts.yes === true

  let options: ScaffoldOptions

  if (nonInteractive) {
    const registryChoice = opts.registry as RegistryChoice | undefined
    options = {
      projectName,
      template: validateTemplate(opts.template as string) ?? "starter",
      pm: validatePm(opts.pm as string) ?? "pnpm",
      flinkVersion: validateFlinkVersion(opts.flinkVersion as string) ?? "2.0",
      gitInit: opts.git !== false,
      installDeps: opts.install !== false,
      registry: registryChoice ? REGISTRY_URLS[registryChoice] : undefined,
    }
  } else {
    const collected = await collectOptions(projectName, opts)
    if (!collected) return
    options = collected
  }

  scaffoldProject(projectDir, options)

  if (options.gitInit) {
    try {
      execSync("git init", { cwd: projectDir, stdio: "ignore" })
    } catch {
      console.warn(pc.yellow("Warning: git init failed. Skipping."))
    }
  }

  if (options.installDeps) {
    try {
      console.log(pc.dim(`Running ${options.pm} install...`))
      execSync(`${options.pm} install`, { cwd: projectDir, stdio: "inherit" })
    } catch {
      console.warn(
        pc.yellow("Warning: dependency installation failed. Run it manually."),
      )
    }
  }

  console.log("")
  console.log(pc.green("Project created successfully!"))
  console.log("")
  console.log(`  ${pc.dim("cd")} ${projectName}`)
  console.log(`  ${pc.dim(options.pm)} run dev`)
  console.log("")
}

async function collectOptions(
  projectName: string,
  cliOpts: Record<string, unknown>,
): Promise<ScaffoldOptions | null> {
  clack.intro(pc.bgCyan(pc.black(" flink-reactor new ")))

  const template = cliOpts.template
    ? (validateTemplate(cliOpts.template as string) ?? "starter")
    : await promptTemplate()

  if (clack.isCancel(template)) {
    clack.cancel("Operation cancelled.")
    return null
  }

  const pm = cliOpts.pm
    ? (validatePm(cliOpts.pm as string) ?? "pnpm")
    : await promptPm()

  if (clack.isCancel(pm)) {
    clack.cancel("Operation cancelled.")
    return null
  }

  const flinkVersion = cliOpts.flinkVersion
    ? (validateFlinkVersion(cliOpts.flinkVersion as string) ?? "2.0")
    : await promptFlinkVersion()

  if (clack.isCancel(flinkVersion)) {
    clack.cancel("Operation cancelled.")
    return null
  }

  let registry: string | undefined
  if (cliOpts.registry) {
    registry = REGISTRY_URLS[cliOpts.registry as RegistryChoice]
  } else {
    const registryChoice = await promptRegistry()
    if (clack.isCancel(registryChoice)) {
      clack.cancel("Operation cancelled.")
      return null
    }
    registry = REGISTRY_URLS[registryChoice as RegistryChoice]
  }

  const gitInit = await clack.confirm({
    message: "Initialize a git repository?",
    initialValue: true,
  })

  if (clack.isCancel(gitInit)) {
    clack.cancel("Operation cancelled.")
    return null
  }

  const installDeps = await clack.confirm({
    message: `Install dependencies with ${pm as string}?`,
    initialValue: true,
  })

  if (clack.isCancel(installDeps)) {
    clack.cancel("Operation cancelled.")
    return null
  }

  clack.outro(pc.green("Scaffolding project..."))

  return {
    projectName,
    template: template as TemplateName,
    pm: pm as PackageManager,
    flinkVersion: flinkVersion as FlinkVersion,
    gitInit: gitInit as boolean,
    installDeps: installDeps as boolean,
    registry,
  }
}

async function promptTemplate(): Promise<TemplateName | symbol> {
  return clack.select({
    message: "Which template would you like to use?",
    options: [
      {
        value: "starter",
        label: "Starter",
        hint: TEMPLATE_DESCRIPTIONS.starter,
      },
      {
        value: "minimal",
        label: "Minimal",
        hint: TEMPLATE_DESCRIPTIONS.minimal,
      },
      {
        value: "cdc-lakehouse",
        label: "CDC to Lakehouse",
        hint: TEMPLATE_DESCRIPTIONS["cdc-lakehouse"],
      },
      {
        value: "realtime-analytics",
        label: "Real-time Analytics",
        hint: TEMPLATE_DESCRIPTIONS["realtime-analytics"],
      },
      {
        value: "monorepo",
        label: "Monorepo",
        hint: TEMPLATE_DESCRIPTIONS.monorepo,
      },
      {
        value: "ecommerce",
        label: "E-Commerce",
        hint: TEMPLATE_DESCRIPTIONS.ecommerce,
      },
      {
        value: "ride-sharing",
        label: "Ride-Sharing",
        hint: TEMPLATE_DESCRIPTIONS["ride-sharing"],
      },
      {
        value: "grocery-delivery",
        label: "Grocery Delivery",
        hint: TEMPLATE_DESCRIPTIONS["grocery-delivery"],
      },
      {
        value: "banking",
        label: "Banking / FinTech",
        hint: TEMPLATE_DESCRIPTIONS.banking,
      },
      {
        value: "iot-factory",
        label: "IoT / Smart Factory",
        hint: TEMPLATE_DESCRIPTIONS["iot-factory"],
      },
      {
        value: "stock-basics",
        label: "Stock examples — basics",
        hint: TEMPLATE_DESCRIPTIONS["stock-basics"],
      },
      {
        value: "stock-ds-easy",
        label: "Stock examples — DataStream migration (easy)",
        hint: TEMPLATE_DESCRIPTIONS["stock-ds-easy"],
      },
      {
        value: "stock-ds-moderate",
        label: "Stock examples — DataStream migration (moderate)",
        hint: TEMPLATE_DESCRIPTIONS["stock-ds-moderate"],
      },
      {
        value: "stock-temporal-topn",
        label: "Stock examples — temporal join + Top-N",
        hint: TEMPLATE_DESCRIPTIONS["stock-temporal-topn"],
      },
    ],
  }) as Promise<TemplateName | symbol>
}

async function promptPm(): Promise<PackageManager | symbol> {
  return clack.select({
    message: "Which package manager?",
    options: [
      { value: "pnpm", label: "pnpm", hint: "recommended" },
      { value: "npm", label: "npm" },
      { value: "yarn", label: "yarn" },
    ],
  }) as Promise<PackageManager | symbol>
}

async function promptFlinkVersion(): Promise<FlinkVersion | symbol> {
  return clack.select({
    message: "Target Flink version?",
    options: [
      { value: "2.0", label: "Flink 2.0", hint: "recommended" },
      { value: "1.20", label: "Flink 1.20 LTS" },
      { value: "2.1", label: "Flink 2.1" },
      { value: "2.2", label: "Flink 2.2" },
    ],
  }) as Promise<FlinkVersion | symbol>
}

async function promptRegistry(): Promise<RegistryChoice | symbol> {
  return clack.select({
    message: "npm registry?",
    options: [
      { value: "npm", label: "npm", hint: "public registry" },
      {
        value: "verdaccio",
        label: "Verdaccio",
        hint: "local registry (localhost:4873)",
      },
    ],
  }) as Promise<RegistryChoice | symbol>
}

function validateTemplate(value: string | undefined): TemplateName | null {
  const valid: TemplateName[] = [
    "starter",
    "minimal",
    "cdc-lakehouse",
    "realtime-analytics",
    "monorepo",
    "ecommerce",
    "ride-sharing",
    "grocery-delivery",
    "banking",
    "iot-factory",
    "lakehouse-ingestion",
    "lakehouse-analytics",
    "stock-basics",
    "stock-ds-easy",
    "stock-ds-moderate",
    "stock-temporal-topn",
  ]
  return valid.includes(value as TemplateName) ? (value as TemplateName) : null
}

function validatePm(value: string | undefined): PackageManager | null {
  const valid: PackageManager[] = ["pnpm", "npm", "yarn"]
  return valid.includes(value as PackageManager)
    ? (value as PackageManager)
    : null
}

function validateFlinkVersion(value: string | undefined): FlinkVersion | null {
  const valid: FlinkVersion[] = ["1.20", "2.0", "2.1", "2.2"]
  return valid.includes(value as FlinkVersion) ? (value as FlinkVersion) : null
}

export function scaffoldProject(
  projectDir: string,
  options: ScaffoldOptions,
): void {
  const factory = TEMPLATE_FACTORIES[options.template]
  const files = factory(options)

  for (const file of files) {
    const filePath = join(projectDir, file.path)
    const dir = join(filePath, "..")
    mkdirSync(dir, { recursive: true })
    writeFileSync(filePath, file.content, "utf-8")
  }
}

export function renderTemplate(
  templateSource: string,
  context: Record<string, unknown>,
): string {
  const compiled = Handlebars.compile(templateSource, { noEscape: true })
  return compiled(context)
}
