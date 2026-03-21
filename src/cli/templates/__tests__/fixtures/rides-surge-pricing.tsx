import { BroadcastJoin } from "@/components/joins"
import { Pipeline } from "@/components/pipeline"
import { KafkaSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { Aggregate } from "@/components/transforms"
import { TumbleWindow } from "@/components/windows"
import { createElement } from "@/core/jsx-runtime"
import { Field, Schema } from "@/core/schema"

const RideRequestSchema = Schema({
  fields: {
    rideId: Field.STRING(),
    riderId: Field.STRING(),
    zoneId: Field.STRING(),
    pickupLat: Field.DOUBLE(),
    pickupLng: Field.DOUBLE(),
    dropoffLat: Field.DOUBLE(),
    dropoffLng: Field.DOUBLE(),
    requestTime: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "requestTime",
    expression: "requestTime - INTERVAL '5' SECOND",
  },
})

const SurgeZoneSchema = Schema({
  fields: {
    zoneId: Field.STRING(),
    baseMultiplier: Field.DOUBLE(),
    updateTime: Field.TIMESTAMP(3),
  },
  primaryKey: { columns: ["zoneId"] },
})

const requests = KafkaSource({
  topic: "rides.requests",
  schema: RideRequestSchema,
  bootstrapServers: "kafka:9092",
  consumerGroup: "rides-surge",
})

const surgeZones = KafkaSource({
  topic: "rides.surge-zones",
  schema: SurgeZoneSchema,
  bootstrapServers: "kafka:9092",
  consumerGroup: "rides-surge-config",
})

const windowed = TumbleWindow({
  size: "1 MINUTE",
  on: "requestTime",
  children: requests,
})

const demand = Aggregate({
  groupBy: ["zoneId"],
  select: {
    zoneId: "zoneId",
    demandCount: "COUNT(*)",
    windowEnd: "window_end",
  },
  children: windowed,
})

const surgeResult = BroadcastJoin({
  left: demand,
  right: surgeZones,
  on: "demand.zoneId = surgeZones.zoneId",
})

export default (
  <Pipeline name="rides-surge-pricing" mode="streaming">
    {requests}
    {surgeZones}
    {surgeResult}
    <KafkaSink topic="rides.surge-prices" bootstrapServers="kafka:9092" />
  </Pipeline>
)
