import type { ScaffoldOptions, TemplateFile } from '../commands/new.js';
import { sharedFiles } from './shared.js';

export function getStarterTemplates(opts: ScaffoldOptions): TemplateFile[] {
  return [
    ...sharedFiles(opts),
    {
      path: 'schemas/events.ts',
      content: `import { Schema, Field } from 'flink-reactor';

export const EventSchema = Schema({
  fields: {
    id: Field.BIGINT(),
    userId: Field.STRING(),
    eventType: Field.STRING(),
    payload: Field.STRING(),
    timestamp: Field.TIMESTAMP(3),
  },
});
`,
    },
    {
      path: 'pipelines/hello-world/index.tsx',
      content: `import { Pipeline, KafkaSource, KafkaSink, Filter } from 'flink-reactor';
import { EventSchema } from '../../schemas/events';

export default (
  <Pipeline name="hello-world">
    <KafkaSource
      topic="events"
      schema={EventSchema}
      bootstrapServers="localhost:9092"
      consumerGroup="hello-world"
    />
    <Filter condition="eventType <> 'internal'" />
    <KafkaSink
      topic="filtered-events"
      bootstrapServers="localhost:9092"
    />
  </Pipeline>
);
`,
    },
    {
      path: 'tests/pipelines/hello-world.test.ts',
      content: `import { describe, it, expect } from 'vitest';
// import { synth } from 'flink-reactor/testing';

describe('hello-world pipeline', () => {
  it.todo('synthesizes valid Flink SQL');
});
`,
    },
  ];
}
