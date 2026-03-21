import { MatchRecognize } from "@/components/cep"
import { IntervalJoin } from "@/components/joins"
import { Pipeline } from "@/components/pipeline"
import { Route } from "@/components/route"
import { JdbcSink, KafkaSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { createElement } from "@/core/jsx-runtime"
import { Field, Schema } from "@/core/schema"

const RideRequestSchema = Schema({
  fields: {
    rideId: Field.STRING(),
    riderId: Field.STRING(),
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

const TripEventSchema = Schema({
  fields: {
    rideId: Field.STRING(),
    driverId: Field.STRING(),
    status: Field.STRING(),
    eventTime: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "eventTime",
    expression: "eventTime - INTERVAL '5' SECOND",
  },
})

const requests = KafkaSource({
  topic: "rides.requests",
  schema: RideRequestSchema,
  bootstrapServers: "kafka:9092",
  consumerGroup: "rides-tracking-req",
})

const events = KafkaSource({
  topic: "rides.trip-events",
  schema: TripEventSchema,
  bootstrapServers: "kafka:9092",
  consumerGroup: "rides-tracking-events",
})

const joined = IntervalJoin({
  left: requests,
  right: events,
  on: "requests.rideId = events.rideId",
  interval: {
    from: "requests.requestTime",
    to: "requests.requestTime + INTERVAL '5' MINUTE",
  },
})

export default (
  <Pipeline name="rides-trip-tracking" mode="streaming">
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
        rideId: "LAST(rideId)",
        driverId: "LAST(driverId)",
        tripStatus: "LAST(status)",
        pickupTime: "FIRST(eventTime, pickup)",
        dropoffTime: "LAST(eventTime, dropoff)",
      },
    })}
    <Route>
      <Route.Branch condition="tripStatus = 'dropoff'">
        <JdbcSink
          table="completed_trips"
          url="jdbc:postgresql://postgres:5432/flink_sink"
        />
      </Route.Branch>
      <Route.Branch condition="tripStatus = 'cancelled'">
        <KafkaSink topic="rides.driver-alerts" bootstrapServers="kafka:9092" />
      </Route.Branch>
    </Route>
  </Pipeline>
)
