import * as clack from "@clack/prompts"
import pc from "picocolors"

export async function waitForServices(opts: {
  flinkPort: number
  sqlGatewayPort: number
  kafkaPort?: number
  postgresPort?: number
  seaweedfsPort?: number
  icebergRestPort?: number
  flussPort?: number
  timeoutMs?: number
  intervalMs?: number
}): Promise<void> {
  const timeout = opts.timeoutMs ?? 60_000
  const interval = opts.intervalMs ?? 2_000

  const spinner = clack.spinner()
  spinner.start("Waiting for services to be ready...")

  const deadline = Date.now() + timeout

  const services: Array<{ name: string; check: () => Promise<boolean> }> = [
    { name: "Flink JobManager", check: () => checkFlink(opts.flinkPort) },
    { name: "SQL Gateway", check: () => checkSqlGateway(opts.sqlGatewayPort) },
  ]
  const { kafkaPort, postgresPort, seaweedfsPort, icebergRestPort, flussPort } =
    opts
  if (kafkaPort !== undefined) {
    services.push({ name: "Kafka", check: () => checkKafka(kafkaPort) })
  }
  if (postgresPort !== undefined) {
    services.push({ name: "PostgreSQL", check: () => checkTcp(postgresPort) })
  }
  if (seaweedfsPort !== undefined) {
    services.push({
      name: "SeaweedFS (S3)",
      check: () => checkTcp(seaweedfsPort),
    })
  }
  if (icebergRestPort !== undefined) {
    services.push({
      name: "Iceberg REST",
      check: () => checkIcebergRest(icebergRestPort),
    })
  }
  if (flussPort !== undefined) {
    services.push({
      name: "Fluss Coordinator",
      check: () => checkTcp(flussPort),
    })
  }

  const ready = new Set<string>()

  while (Date.now() < deadline) {
    for (const svc of services) {
      if (ready.has(svc.name)) continue
      try {
        const ok = await svc.check()
        if (ok) {
          ready.add(svc.name)
          spinner.message(
            `${svc.name} ready (${ready.size}/${services.length})...`,
          )
        }
      } catch {
        // Not ready yet
      }
    }

    if (ready.size === services.length) {
      spinner.stop(pc.green("All services ready."))
      return
    }

    await sleep(interval)
  }

  const notReady = services.filter((s) => !ready.has(s.name)).map((s) => s.name)
  spinner.stop(pc.red(`Timed out waiting for: ${notReady.join(", ")}`))
  throw new Error(
    `Services not ready after ${timeout / 1000}s: ${notReady.join(", ")}`,
  )
}

export async function isClusterRunning(flinkPort: number): Promise<boolean> {
  return checkFlink(flinkPort)
}

async function checkFlink(port: number): Promise<boolean> {
  try {
    const res = await fetch(`http://localhost:${port}/overview`)
    return res.ok
  } catch {
    return false
  }
}

async function checkSqlGateway(port: number): Promise<boolean> {
  try {
    const res = await fetch(`http://localhost:${port}/info`)
    return res.ok
  } catch {
    return false
  }
}

async function checkIcebergRest(port: number): Promise<boolean> {
  try {
    const res = await fetch(`http://localhost:${port}/health`)
    return res.ok
  } catch {
    return false
  }
}

async function checkKafka(port: number): Promise<boolean> {
  return checkTcp(port)
}

async function checkTcp(port: number): Promise<boolean> {
  const { createConnection } = await import("node:net")

  return new Promise<boolean>((resolve) => {
    const socket = createConnection({ host: "localhost", port }, () => {
      socket.destroy()
      resolve(true)
    })
    socket.on("error", () => {
      socket.destroy()
      resolve(false)
    })
    socket.setTimeout(2000, () => {
      socket.destroy()
      resolve(false)
    })
  })
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
