import { execSync } from 'node:child_process';
import pc from 'picocolors';

export interface CdcPublishOptions {
  mode: 'batch' | 'continuous';
  composeFile?: string;
  topic?: string;
  intervalMs?: number;
  signal?: AbortSignal;
}

interface Product {
  id: number;
  name: string;
  category: string;
  price: number;
  quantity: number;
}

const SEED_PRODUCTS: Product[] = [
  { id: 1001, name: 'Wireless Mouse', category: 'peripherals', price: 29.99, quantity: 150 },
  { id: 1002, name: 'Mechanical Keyboard', category: 'peripherals', price: 89.99, quantity: 80 },
  { id: 1003, name: '27" Monitor', category: 'displays', price: 349.99, quantity: 45 },
  { id: 1004, name: 'USB-C Hub', category: 'accessories', price: 49.99, quantity: 200 },
  { id: 1005, name: 'Webcam HD', category: 'peripherals', price: 79.99, quantity: 120 },
  { id: 1006, name: 'Standing Desk', category: 'furniture', price: 599.99, quantity: 30 },
  { id: 1007, name: 'Ergonomic Chair', category: 'furniture', price: 449.99, quantity: 25 },
  { id: 1008, name: 'Laptop Stand', category: 'accessories', price: 39.99, quantity: 180 },
  { id: 1009, name: 'Noise-Cancel Headphones', category: 'audio', price: 249.99, quantity: 60 },
  { id: 1010, name: 'Bluetooth Speaker', category: 'audio', price: 119.99, quantity: 90 },
];

export async function publishCdcMessages(opts: CdcPublishOptions): Promise<void> {
  const composeFile = opts.composeFile;
  const topic = opts.topic ?? 'cdc.inventory.products';
  const intervalMs = opts.intervalMs ?? 5_000;

  if (opts.mode === 'batch') {
    await publishBatch(composeFile, topic);
  } else {
    await publishContinuous(composeFile, topic, intervalMs, opts.signal);
  }
}

async function publishBatch(composeFile: string | undefined, topic: string): Promise<void> {
  const messages: string[] = [];

  // 10 inserts
  for (const product of SEED_PRODUCTS) {
    messages.push(
      JSON.stringify(createDebeziumEnvelope('c', null, product)),
    );
  }

  // 20 updates (2 per product: random price/quantity changes)
  for (const product of SEED_PRODUCTS) {
    const before1 = { ...product };
    const after1 = { ...product, price: round(product.price * (0.9 + Math.random() * 0.2)), quantity: product.quantity + randomInt(-10, 10) };
    messages.push(JSON.stringify(createDebeziumEnvelope('u', before1, after1)));

    const before2 = { ...after1 };
    const after2 = { ...after1, quantity: Math.max(0, after1.quantity + randomInt(-20, 30)) };
    messages.push(JSON.stringify(createDebeziumEnvelope('u', before2, after2)));
  }

  // 2 deletes (last two products)
  for (const product of SEED_PRODUCTS.slice(-2)) {
    messages.push(
      JSON.stringify(createDebeziumEnvelope('d', product, null)),
    );
  }

  publishToKafka(composeFile, topic, messages);
  console.log(`  ${pc.green('✓')} Published ${messages.length} CDC messages to ${pc.dim(topic)}`);
}

async function publishContinuous(
  composeFile: string | undefined,
  topic: string,
  intervalMs: number,
  signal?: AbortSignal,
): Promise<void> {
  const activeProducts = [...SEED_PRODUCTS.slice(0, -2)]; // Exclude deleted products
  let counter = 0;

  while (!signal?.aborted) {
    const product = activeProducts[counter % activeProducts.length];
    const before = { ...product };
    const after = {
      ...product,
      price: round(product.price * (0.9 + Math.random() * 0.2)),
      quantity: Math.max(0, product.quantity + randomInt(-5, 15)),
    };

    // Mutate the active copy so subsequent updates build on prior state
    Object.assign(product, after);

    const message = JSON.stringify(createDebeziumEnvelope('u', before, after));
    try {
      publishToKafka(composeFile, topic, [message]);
    } catch {
      // Container might be stopped — exit gracefully
      break;
    }

    counter++;
    await sleep(intervalMs, signal);
  }
}

function createDebeziumEnvelope(
  op: 'c' | 'u' | 'd',
  before: Product | null,
  after: Product | null,
): object {
  return {
    before: before
      ? {
          id: before.id,
          name: before.name,
          category: before.category,
          price: before.price,
          quantity: before.quantity,
        }
      : null,
    after: after
      ? {
          id: after.id,
          name: after.name,
          category: after.category,
          price: after.price,
          quantity: after.quantity,
        }
      : null,
    op,
    ts_ms: Date.now(),
    source: {
      version: '2.5.0',
      connector: 'mysql',
      name: 'inventory',
      db: 'inventory',
      table: 'products',
    },
  };
}

function publishToKafka(composeFile: string | undefined, topic: string, messages: string[]): void {
  // Pipe messages to kafka-console-producer via docker compose exec (uses service names)
  const input = messages.join('\n');
  const composeArgs = composeFile ? `docker compose -f "${composeFile}" exec -T` : 'docker compose exec -T';
  execSync(
    `${composeArgs} kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic ${topic}`,
    {
      input,
      stdio: ['pipe', 'pipe', 'pipe'],
      timeout: 10_000,
    },
  );
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    signal?.addEventListener('abort', () => {
      clearTimeout(timer);
      resolve();
    }, { once: true });
  });
}

function randomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function round(n: number): number {
  return Math.round(n * 100) / 100;
}
