import { execSync } from "node:child_process"
import pc from "picocolors"

export type CdcDomain = "ecommerce" | "iot" | "all"

export interface CdcPublishOptions {
  mode: "batch" | "continuous"
  domain?: CdcDomain
  composeFile?: string
  topic?: string
  intervalMs?: number
  signal?: AbortSignal
}

// ── Ecommerce domain ────────────────────────────────────────────────

interface Product {
  id: number
  name: string
  category: string
  price: number
  quantity: number
}

const SEED_PRODUCTS: Product[] = [
  {
    id: 1001,
    name: "Wireless Mouse",
    category: "peripherals",
    price: 29.99,
    quantity: 150,
  },
  {
    id: 1002,
    name: "Mechanical Keyboard",
    category: "peripherals",
    price: 89.99,
    quantity: 80,
  },
  {
    id: 1003,
    name: '27" Monitor',
    category: "displays",
    price: 349.99,
    quantity: 45,
  },
  {
    id: 1004,
    name: "USB-C Hub",
    category: "accessories",
    price: 49.99,
    quantity: 200,
  },
  {
    id: 1005,
    name: "Webcam HD",
    category: "peripherals",
    price: 79.99,
    quantity: 120,
  },
  {
    id: 1006,
    name: "Standing Desk",
    category: "furniture",
    price: 599.99,
    quantity: 30,
  },
  {
    id: 1007,
    name: "Ergonomic Chair",
    category: "furniture",
    price: 449.99,
    quantity: 25,
  },
  {
    id: 1008,
    name: "Laptop Stand",
    category: "accessories",
    price: 39.99,
    quantity: 180,
  },
  {
    id: 1009,
    name: "Noise-Cancel Headphones",
    category: "audio",
    price: 249.99,
    quantity: 60,
  },
  {
    id: 1010,
    name: "Bluetooth Speaker",
    category: "audio",
    price: 119.99,
    quantity: 90,
  },
]

// ── IoT domain ──────────────────────────────────────────────────────

interface IoTDevice {
  device_id: string
  device_name: string
  device_type: string
  location: string
  firmware: string
  status: string
}

const IOT_DEVICES: IoTDevice[] = [
  {
    device_id: "dev-001",
    device_name: "Warehouse-TempSensor-A1",
    device_type: "temperature",
    location: "warehouse-a",
    firmware: "2.1.0",
    status: "active",
  },
  {
    device_id: "dev-002",
    device_name: "Warehouse-HumiditySensor-A2",
    device_type: "humidity",
    location: "warehouse-a",
    firmware: "2.1.0",
    status: "active",
  },
  {
    device_id: "dev-003",
    device_name: "Factory-PressureSensor-B1",
    device_type: "pressure",
    location: "factory-b",
    firmware: "2.0.3",
    status: "active",
  },
  {
    device_id: "dev-004",
    device_name: "Factory-VibrationSensor-B2",
    device_type: "vibration",
    location: "factory-b",
    firmware: "2.0.3",
    status: "active",
  },
  {
    device_id: "dev-005",
    device_name: "Office-TempSensor-C1",
    device_type: "temperature",
    location: "office-c",
    firmware: "2.1.0",
    status: "active",
  },
  {
    device_id: "dev-006",
    device_name: "Office-HumiditySensor-C2",
    device_type: "humidity",
    location: "office-c",
    firmware: "2.1.0",
    status: "active",
  },
  {
    device_id: "dev-007",
    device_name: "Dock-TempSensor-D1",
    device_type: "temperature",
    location: "loading-dock-d",
    firmware: "1.9.5",
    status: "active",
  },
  {
    device_id: "dev-008",
    device_name: "Dock-PressureSensor-D2",
    device_type: "pressure",
    location: "loading-dock-d",
    firmware: "1.9.5",
    status: "active",
  },
  {
    device_id: "dev-009",
    device_name: "Warehouse-VibrationSensor-A3",
    device_type: "vibration",
    location: "warehouse-a",
    firmware: "2.0.3",
    status: "active",
  },
  {
    device_id: "dev-010",
    device_name: "Factory-TempSensor-B3",
    device_type: "temperature",
    location: "factory-b",
    firmware: "2.1.0",
    status: "active",
  },
]

interface SensorReading {
  device_id: string
  sensor_type: string
  value: number
  unit: string
  reading_time: string
  location: string
}

const SENSOR_RANGES: Record<
  string,
  { min: number; max: number; unit: string }
> = {
  temperature: { min: 15, max: 95, unit: "°C" },
  humidity: { min: 10, max: 90, unit: "%" },
  pressure: { min: 980, max: 1050, unit: "hPa" },
  vibration: { min: 0, max: 50, unit: "mm/s" },
}

// ── Main entry point ────────────────────────────────────────────────

export async function publishCdcMessages(
  opts: CdcPublishOptions,
): Promise<void> {
  const composeFile = opts.composeFile
  const domain = opts.domain ?? "all"

  if (opts.mode === "batch") {
    if (domain === "ecommerce" || domain === "all") {
      const topic = opts.topic ?? "cdc.inventory.products"
      await publishEcommerceBatch(composeFile, topic)
    }
    if (domain === "iot" || domain === "all") {
      await publishIoTBatch(composeFile)
    }
  } else {
    const intervalMs = opts.intervalMs ?? 5_000
    if (domain === "ecommerce" || domain === "all") {
      const topic = opts.topic ?? "cdc.inventory.products"
      // Run ecommerce continuous in background if both domains
      if (domain === "all") {
        publishEcommerceContinuous(composeFile, topic, intervalMs, opts.signal)
        await publishIoTContinuous(composeFile, 3_000, opts.signal)
      } else {
        await publishEcommerceContinuous(
          composeFile,
          topic,
          intervalMs,
          opts.signal,
        )
      }
    } else if (domain === "iot") {
      await publishIoTContinuous(composeFile, 3_000, opts.signal)
    }
  }
}

// ── Ecommerce batch/continuous ──────────────────────────────────────

async function publishEcommerceBatch(
  composeFile: string | undefined,
  topic: string,
): Promise<void> {
  const messages: string[] = []

  // 10 inserts
  for (const product of SEED_PRODUCTS) {
    messages.push(
      JSON.stringify(
        createDebeziumEnvelope("c", null, product, "inventory", "products"),
      ),
    )
  }

  // 20 updates (2 per product: random price/quantity changes)
  for (const product of SEED_PRODUCTS) {
    const before1 = { ...product }
    const after1 = {
      ...product,
      price: round(product.price * (0.9 + Math.random() * 0.2)),
      quantity: product.quantity + randomInt(-10, 10),
    }
    messages.push(
      JSON.stringify(
        createDebeziumEnvelope("u", before1, after1, "inventory", "products"),
      ),
    )

    const before2 = { ...after1 }
    const after2 = {
      ...after1,
      quantity: Math.max(0, after1.quantity + randomInt(-20, 30)),
    }
    messages.push(
      JSON.stringify(
        createDebeziumEnvelope("u", before2, after2, "inventory", "products"),
      ),
    )
  }

  // 2 deletes (last two products)
  for (const product of SEED_PRODUCTS.slice(-2)) {
    messages.push(
      JSON.stringify(
        createDebeziumEnvelope("d", product, null, "inventory", "products"),
      ),
    )
  }

  publishToKafka(composeFile, topic, messages)
  console.log(
    `  ${pc.green("✓")} Published ${messages.length} CDC messages to ${pc.dim(topic)}`,
  )
}

async function publishEcommerceContinuous(
  composeFile: string | undefined,
  topic: string,
  intervalMs: number,
  signal?: AbortSignal,
): Promise<void> {
  const activeProducts = [...SEED_PRODUCTS.slice(0, -2)] // Exclude deleted products
  let counter = 0

  while (!signal?.aborted) {
    const product = activeProducts[counter % activeProducts.length]
    const before = { ...product }
    const after = {
      ...product,
      price: round(product.price * (0.9 + Math.random() * 0.2)),
      quantity: Math.max(0, product.quantity + randomInt(-5, 15)),
    }

    // Mutate the active copy so subsequent updates build on prior state
    Object.assign(product, after)

    const message = JSON.stringify(
      createDebeziumEnvelope("u", before, after, "inventory", "products"),
    )
    try {
      publishToKafka(composeFile, topic, [message])
    } catch {
      // Container might be stopped — exit gracefully
      break
    }

    counter++
    await sleep(intervalMs, signal)
  }
}

// ── IoT batch/continuous ────────────────────────────────────────────

async function publishIoTBatch(composeFile: string | undefined): Promise<void> {
  const deviceTopic = "iot.registry.devices"
  const telemetryTopic = "iot.telemetry.readings"

  // 10 device creates
  const deviceMessages: string[] = []
  for (const device of IOT_DEVICES) {
    deviceMessages.push(
      JSON.stringify(
        createDebeziumEnvelope("c", null, device, "iot", "devices"),
      ),
    )
  }
  publishToKafka(composeFile, deviceTopic, deviceMessages)
  console.log(
    `  ${pc.green("✓")} Published ${deviceMessages.length} device creates to ${pc.dim(deviceTopic)}`,
  )

  // 20 telemetry readings (2 per device)
  const telemetryMessages: string[] = []
  for (const device of IOT_DEVICES) {
    for (let i = 0; i < 2; i++) {
      const reading = generateReading(device)
      telemetryMessages.push(
        JSON.stringify(
          createDebeziumEnvelope("c", null, reading, "iot", "telemetry"),
        ),
      )
    }
  }
  publishToKafka(composeFile, telemetryTopic, telemetryMessages)
  console.log(
    `  ${pc.green("✓")} Published ${telemetryMessages.length} telemetry readings to ${pc.dim(telemetryTopic)}`,
  )

  // 10 device updates (firmware bumps, status changes)
  const updateMessages: string[] = []
  for (const device of IOT_DEVICES) {
    const before = { ...device }
    const after = { ...device, firmware: "2.2.0" }
    updateMessages.push(
      JSON.stringify(
        createDebeziumEnvelope("u", before, after, "iot", "devices"),
      ),
    )
  }

  // 2 device decommissions (last two devices)
  for (const device of IOT_DEVICES.slice(-2)) {
    const updated = { ...device, firmware: "2.2.0" }
    const decommissioned = { ...updated, status: "decommissioned" }
    updateMessages.push(
      JSON.stringify(
        createDebeziumEnvelope("u", updated, decommissioned, "iot", "devices"),
      ),
    )
  }
  publishToKafka(composeFile, deviceTopic, updateMessages)
  console.log(
    `  ${pc.green("✓")} Published ${updateMessages.length} device updates to ${pc.dim(deviceTopic)}`,
  )
}

async function publishIoTContinuous(
  composeFile: string | undefined,
  intervalMs: number,
  signal?: AbortSignal,
): Promise<void> {
  const activeDevices = [...IOT_DEVICES.slice(0, -2)] // Exclude decommissioned
  const telemetryTopic = "iot.telemetry.readings"
  let counter = 0

  while (!signal?.aborted) {
    const device = activeDevices[counter % activeDevices.length]
    const reading = generateReading(device)
    const message = JSON.stringify(
      createDebeziumEnvelope("c", null, reading, "iot", "telemetry"),
    )

    try {
      publishToKafka(composeFile, telemetryTopic, [message])
    } catch {
      break
    }

    counter++
    await sleep(intervalMs, signal)
  }
}

// ── Shared helpers ──────────────────────────────────────────────────

function generateReading(device: IoTDevice): SensorReading {
  const range = SENSOR_RANGES[device.device_type]!
  return {
    device_id: device.device_id,
    sensor_type: device.device_type,
    value: round(range.min + Math.random() * (range.max - range.min)),
    unit: range.unit,
    reading_time: new Date().toISOString().replace("T", " ").slice(0, 23),
    location: device.location,
  }
}

function createDebeziumEnvelope<T extends object>(
  op: "c" | "u" | "d",
  before: T | null,
  after: T | null,
  db: string,
  table: string,
): object {
  return {
    before: before ? { ...before } : null,
    after: after ? { ...after } : null,
    op,
    ts_ms: Date.now(),
    source: {
      version: "2.5.0",
      connector: db === "iot" ? "postgresql" : "mysql",
      name: db,
      db,
      table,
    },
  }
}

function publishToKafka(
  composeFile: string | undefined,
  topic: string,
  messages: string[],
): void {
  // Pipe messages to kafka-console-producer via docker compose exec (uses service names)
  const input = messages.join("\n")
  const composeArgs = composeFile
    ? `docker compose -f "${composeFile}" exec -T`
    : "docker compose exec -T"
  execSync(
    `${composeArgs} kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic ${topic}`,
    {
      input,
      stdio: ["pipe", "pipe", "pipe"],
      timeout: 10_000,
    },
  )
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms)
    signal?.addEventListener(
      "abort",
      () => {
        clearTimeout(timer)
        resolve()
      },
      { once: true },
    )
  })
}

function randomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min
}

function round(n: number): number {
  return Math.round(n * 100) / 100
}
