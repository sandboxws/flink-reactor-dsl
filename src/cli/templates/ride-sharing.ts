import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import { sharedFiles } from "./shared.js"

export function getRideSharingTemplates(opts: ScaffoldOptions): TemplateFile[] {
  return [
    ...sharedFiles(opts),
    {
      path: "flink-reactor.config.ts",
      content: `import { defineConfig } from '@flink-reactor/dsl';

export default defineConfig({
  flink: { version: '${opts.flinkVersion}' },

  environments: {
    minikube: {
      cluster: { url: 'http://localhost:8081' },
      kafka: { bootstrapServers: 'kafka:9092' },
      sim: {
        init: {
          kafka: {
            topics: [
              'rides.requests',
              'rides.trip-events',
              'rides.surge-zones',
              'rides.surge-prices',
              'rides.driver-alerts',
            ],
          },
        },
      },
      pipelines: { '*': { parallelism: 2 } },
    },
    production: {
      cluster: { url: 'https://flink-prod:8081' },
      kubernetes: { namespace: 'flink-prod' },
      pipelines: { '*': { parallelism: 4 } },
    },
  },
});
`,
    },
    {
      path: "schemas/rides.ts",
      content: `import { Schema, Field } from '@flink-reactor/dsl';

export const RideRequestSchema = Schema({
  fields: {
    rideId: Field.STRING(),
    riderId: Field.STRING(),
    pickupLat: Field.DOUBLE(),
    pickupLng: Field.DOUBLE(),
    dropoffLat: Field.DOUBLE(),
    dropoffLng: Field.DOUBLE(),
    requestTime: Field.TIMESTAMP(3),
  },
  watermark: { column: 'requestTime', expression: "requestTime - INTERVAL '5' SECOND" },
});

export const TripEventSchema = Schema({
  fields: {
    rideId: Field.STRING(),
    driverId: Field.STRING(),
    status: Field.STRING(),
    eventTime: Field.TIMESTAMP(3),
  },
  watermark: { column: 'eventTime', expression: "eventTime - INTERVAL '5' SECOND" },
});

export const SurgeZoneSchema = Schema({
  fields: {
    zoneId: Field.STRING(),
    baseMultiplier: Field.DOUBLE(),
    updateTime: Field.TIMESTAMP(3),
  },
  primaryKey: { columns: ['zoneId'] },
});
`,
    },
    {
      path: "pipelines/rides-trip-tracking/index.tsx",
      content: `import {
  Pipeline, KafkaSource, KafkaSink, JdbcSink,
  IntervalJoin, MatchRecognize, Route,
} from '@flink-reactor/dsl';
import { RideRequestSchema, TripEventSchema } from '@/schemas/rides';

const requests = KafkaSource({
  topic: "rides.requests",
  schema: RideRequestSchema,
  bootstrapServers: "kafka:9092",
  consumerGroup: "rides-tracking-req",
});

const events = KafkaSource({
  topic: "rides.trip-events",
  schema: TripEventSchema,
  bootstrapServers: "kafka:9092",
  consumerGroup: "rides-tracking-events",
});

const joined = IntervalJoin({
  left: requests,
  right: events,
  on: "requests.rideId = events.rideId",
  interval: { from: "requests.requestTime", to: "requests.requestTime + INTERVAL '5' MINUTE" },
});

export default (
  <Pipeline
    name="rides-trip-tracking"
    mode="streaming"
    parallelism={4}
    stateBackend="rocksdb"
    checkpoint={{ interval: "30s", mode: "exactly-once" }}
    flinkConfig={{
      "state.checkpoints.dir": "s3://flink-state/checkpoints/rides-trip-tracking",
      "state.savepoints.dir": "s3://flink-state/savepoints/rides-trip-tracking",
      "s3.endpoint": "http://seaweedfs.flink-demo.svc:8333",
      "s3.path.style.access": "true",
    }}
  >
    {requests}
    {events}
    {MatchRecognize({
      input: joined,
      pattern: "request accept? pickup dropoff",
      define: {
        request: "status = 'requested'",
        accept: "status = 'accepted'",
        pickup: "status = 'pickup'",
        dropoff: "status = 'dropoff'",
      },
      measures: {
        rideId: 'LAST(rideId)',
        driverId: 'LAST(driverId)',
        tripStatus: 'LAST(status)',
        pickupTime: "FIRST(eventTime, pickup)",
        dropoffTime: "LAST(eventTime, dropoff)",
      },
    })}
    <Route>
      <Route.Branch condition="tripStatus = 'dropoff'">
        <JdbcSink table="completed_trips" url="jdbc:postgresql://postgres:5432/flink_sink" />
      </Route.Branch>
      <Route.Branch condition="tripStatus = 'cancelled'">
        <KafkaSink topic="rides.driver-alerts" bootstrapServers="kafka:9092" />
      </Route.Branch>
    </Route>
  </Pipeline>
);
`,
    },
    {
      path: "pipelines/rides-surge-pricing/index.tsx",
      content: `import {
  Pipeline, KafkaSource, KafkaSink,
  TumbleWindow, Aggregate, BroadcastJoin,
} from '@flink-reactor/dsl';
import { RideRequestSchema, SurgeZoneSchema } from '@/schemas/rides';

const requests = KafkaSource({
  topic: "rides.requests",
  schema: RideRequestSchema,
  bootstrapServers: "kafka:9092",
  consumerGroup: "rides-surge",
});

const surgeZones = KafkaSource({
  topic: "rides.surge-zones",
  schema: SurgeZoneSchema,
  bootstrapServers: "kafka:9092",
  consumerGroup: "rides-surge-config",
});

const windowed = TumbleWindow({ size: "1 MINUTE", on: "requestTime", children: requests });

const demand = Aggregate({
  groupBy: ['zoneId'],
  select: { zoneId: 'zoneId', demandCount: 'COUNT(*)', windowEnd: 'WINDOW_END' },
  children: windowed,
});

const surgeResult = BroadcastJoin({
  left: demand,
  right: surgeZones,
  on: "demand.zoneId = surgeZones.zoneId",
});

export default (
  <Pipeline
    name="rides-surge-pricing"
    mode="streaming"
    parallelism={4}
    stateBackend="rocksdb"
    checkpoint={{ interval: "30s", mode: "exactly-once" }}
    flinkConfig={{
      "state.checkpoints.dir": "s3://flink-state/checkpoints/rides-surge-pricing",
      "state.savepoints.dir": "s3://flink-state/savepoints/rides-surge-pricing",
      "s3.endpoint": "http://seaweedfs.flink-demo.svc:8333",
      "s3.path.style.access": "true",
    }}
  >
    {requests}
    {surgeZones}
    {surgeResult}
    <KafkaSink topic="rides.surge-prices" bootstrapServers="kafka:9092" />
  </Pipeline>
);
`,
    },
  ]
}
