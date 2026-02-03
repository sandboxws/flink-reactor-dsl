import type { ScaffoldOptions, TemplateFile } from '../commands/new.js';
import { sharedFiles } from './shared.js';

export function getRealtimeAnalyticsTemplates(opts: ScaffoldOptions): TemplateFile[] {
  return [
    ...sharedFiles(opts),
    {
      path: 'schemas/page-views.ts',
      content: `import { Schema, Field } from 'flink-reactor';

export const PageViewSchema = Schema('page_views', {
  userId: Field.STRING(),
  pageUrl: Field.STRING(),
  viewTimestamp: Field.TIMESTAMP(3),
});

export const PageViewStatsSchema = Schema('page_view_stats', {
  pageUrl: Field.STRING(),
  viewCount: Field.BIGINT(),
  windowStart: Field.TIMESTAMP(3),
  windowEnd: Field.TIMESTAMP(3),
});
`,
    },
    {
      path: 'pipelines/page-view-analytics/index.tsx',
      content: `import { createElement, Pipeline, KafkaSource, TumbleWindow, Aggregate, JdbcSink } from 'flink-reactor';
import { PageViewSchema, PageViewStatsSchema } from '../../schemas/page-views';

export default (
  <Pipeline name="page-view-analytics">
    <KafkaSource
      topic="page-views"
      schema={PageViewSchema}
      properties={{ 'bootstrap.servers': 'localhost:9092', 'group.id': 'analytics' }}
    />
    <TumbleWindow size="1 MINUTE" on="viewTimestamp" />
    <Aggregate
      groupBy={['pageUrl']}
      select={{
        pageUrl: 'pageUrl',
        viewCount: 'COUNT(*)',
        windowStart: 'WINDOW_START',
        windowEnd: 'WINDOW_END',
      }}
    />
    <JdbcSink
      table="page_view_stats"
      schema={PageViewStatsSchema}
      url="jdbc:postgresql://localhost:5432/analytics"
      driver="org.postgresql.Driver"
    />
  </Pipeline>
);
`,
    },
    {
      path: 'tests/pipelines/page-view-analytics.test.ts',
      content: `import { describe, it, expect } from 'vitest';
// import { synth } from 'flink-reactor/testing';

describe('page-view-analytics pipeline', () => {
  it.todo('synthesizes valid Flink SQL with windowed aggregation');
});
`,
    },
  ];
}
