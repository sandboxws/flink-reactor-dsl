import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import {
  pipelineReadme,
  sharedFiles,
  templatePipelineTestStub,
  templateReadme,
} from "./shared.js"

export function getRealtimeAnalyticsTemplates(
  opts: ScaffoldOptions,
): TemplateFile[] {
  return [
    ...sharedFiles(opts),
    {
      path: "flink-reactor.config.ts",
      content: `import { defineConfig } from '@flink-reactor/dsl';

export default defineConfig({
  flink: { version: '${opts.flinkVersion}' },

  // Kafka for the source stream, Postgres for the JDBC sinks.
  services: { kafka: {}, postgres: {} },

  environments: {
    minikube: {
      cluster: { url: 'http://localhost:8081' },
      kafka: { bootstrapServers: 'kafka:9092' },
      sim: {
        init: {
          kafka: {
            catalogs: [
              {
                name: 'analytics',
                tables: [
                  {
                    table: 'page_views',
                    topic: 'page-views',
                    columns: {
                      userId: 'STRING',
                      pageUrl: 'STRING',
                      viewTimestamp: 'TIMESTAMP(3)',
                    },
                    format: 'json',
                    watermark: { column: 'viewTimestamp', expression: "viewTimestamp - INTERVAL '5' SECOND" },
                  },
                ],
              },
            ],
          },
          jdbc: {
            catalogs: [
              {
                name: 'flink_sink',
                baseUrl: 'jdbc:postgresql://postgres:5432/',
                defaultDatabase: 'analytics',
              },
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
      path: "schemas/page-views.ts",
      content: `import { Schema, Field } from '@flink-reactor/dsl';

export const PageViewSchema = Schema({
  fields: {
    userId: Field.STRING(),
    pageUrl: Field.STRING(),
    viewTimestamp: Field.TIMESTAMP(3),
  },
  watermark: { column: 'viewTimestamp', expression: 'viewTimestamp - INTERVAL \\'5\\' SECOND' },
});

export const PageViewStatsSchema = Schema({
  fields: {
    pageUrl: Field.STRING(),
    viewCount: Field.BIGINT(),
    windowStart: Field.TIMESTAMP(3),
    windowEnd: Field.TIMESTAMP(3),
  },
});
`,
    },
    {
      path: "pipelines/page-view-analytics/index.tsx",
      content: `import { Pipeline, KafkaSource, TumbleWindow, Aggregate, JdbcSink } from '@flink-reactor/dsl';
import { PageViewSchema } from '@/schemas/page-views';

export default (
  <Pipeline name="page-view-analytics">
    <KafkaSource
      topic="page-views"
      schema={PageViewSchema}
      bootstrapServers="kafka:9092"
      consumerGroup="analytics"
    />
    <TumbleWindow size="1 MINUTE" on="viewTimestamp" />
    <Aggregate
      groupBy={['pageUrl']}
      select={{
        pageUrl: 'pageUrl',
        viewCount: 'COUNT(*)',
        windowStart: 'window_start',
        windowEnd: 'window_end',
      }}
    />
    <JdbcSink
      table="page_view_stats"
      url="jdbc:postgresql://postgres:5432/analytics"
    />
  </Pipeline>
);
`,
    },
    pipelineReadme({
      pipelineName: "page-view-analytics",
      tagline:
        "Per-URL view counts in 1-minute event-time tumbling windows, written to Postgres via JDBC.",
      demonstrates: [
        "`<KafkaSource>` reading a JSON page-view stream with a watermark on `viewTimestamp`.",
        '`<TumbleWindow size="1 MINUTE" on="viewTimestamp">` producing fixed event-time buckets.',
        "`<Aggregate>` emitting `COUNT(*)` per `pageUrl` plus `window_start` / `window_end` metadata.",
        "`<JdbcSink>` writing the per-window stats to a Postgres `page_view_stats` table.",
      ],
      topology: `KafkaSource (page-views, json, watermark on viewTimestamp)
  └── TumbleWindow (1 MINUTE, on=viewTimestamp)
        └── Aggregate (GROUP BY pageUrl — COUNT(*), window_start, window_end)
              └── JdbcSink (postgres analytics.page_view_stats)`,
      schemas: [
        "`schemas/page-views.ts` — `PageViewSchema` with watermark on `viewTimestamp`; `PageViewStatsSchema` for the per-window output shape",
      ],
      runCommand: `pnpm synth
pnpm test`,
    }),
    templatePipelineTestStub({
      pipelineName: "page-view-analytics",
      loadBearingPatterns: [/TUMBLE\(/i, /GROUP BY/i, /jdbc/i],
    }),
    templateReadme({
      templateName: "realtime-analytics",
      tagline:
        "Continuous per-URL traffic analytics: Kafka page-views → 1-minute tumbling windows → Postgres. Demonstrates the canonical event-time + watermark + windowed-aggregate + JDBC-sink pattern.",
      pipelines: [
        {
          name: "page-view-analytics",
          pitch:
            "1-minute tumbling-window page-view counts, written to Postgres via JDBC.",
        },
      ],
      gettingStarted: ["pnpm install", "pnpm synth", "pnpm test"],
    }),
  ]
}
