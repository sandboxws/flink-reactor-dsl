import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import { sharedFiles } from "./shared.js"

export function getRideSharingTemplates(opts: ScaffoldOptions): TemplateFile[] {
  return [
    ...sharedFiles(opts),
    {
      path: "schemas/rides.ts",
      content: `import { Schema, Field } from 'flink-reactor';

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
} from 'flink-reactor';
import { RideRequestSchema, TripEventSchema } from '@/schemas/rides';

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
    <KafkaSource topic="rides.requests" schema={RideRequestSchema} bootstrapServers="kafka:9092" consumerGroup="rides-tracking-req" />
    <IntervalJoin
      rightSource={<KafkaSource topic="rides.trip-events" schema={TripEventSchema} bootstrapServers="kafka:9092" consumerGroup="rides-tracking-events" />}
      on="rideId"
      between={{ lower: "-5 MINUTE", upper: "5 MINUTE" }}
    />
    <MatchRecognize
      pattern="request accept? pickup dropoff"
      define={{
        request: "status = 'requested'",
        accept: "status = 'accepted'",
        pickup: "status = 'pickup'",
        dropoff: "status = 'dropoff'",
      }}
    />
    <Route>
      <JdbcSink table="completed_trips" url="jdbc:postgresql://postgres:5432/flink_sink" condition="status = 'dropoff'" />
      <KafkaSink topic="rides.driver-alerts" bootstrapServers="kafka:9092" condition="status = 'cancelled'" />
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
} from 'flink-reactor';
import { RideRequestSchema, SurgeZoneSchema } from '@/schemas/rides';

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
    <KafkaSource topic="rides.requests" schema={RideRequestSchema} bootstrapServers="kafka:9092" consumerGroup="rides-surge" />
    <TumbleWindow size="1 MINUTE" on="requestTime" />
    <Aggregate groupBy={['zoneId']} select={{ zoneId: 'zoneId', demandCount: 'COUNT(*)', windowEnd: 'WINDOW_END' }} />
    <BroadcastJoin
      rightSource={<KafkaSource topic="rides.surge-zones" schema={SurgeZoneSchema} bootstrapServers="kafka:9092" consumerGroup="rides-surge-config" />}
      on="zoneId"
    />
    <KafkaSink topic="rides.surge-zones" bootstrapServers="kafka:9092" />
  </Pipeline>
);
`,
    },
  ]
}
