import { describe, it, expect, beforeEach } from 'vitest';
import { resetNodeIdCounter } from '../../core/jsx-runtime.js';
import { Schema, Field } from '../../core/schema.js';
import { KafkaSource } from '../../components/sources.js';
import { KafkaSink } from '../../components/sinks.js';
import { Filter } from '../../components/transforms.js';
import { Rename, Drop, Cast, Coalesce, AddField } from '../../components/field-transforms.js';
import { Pipeline } from '../../components/pipeline.js';
import { generateSql } from '../sql-generator.js';
import { introspectPipelineSchemas } from '../schema-introspect.js';

beforeEach(() => {
  resetNodeIdCounter();
});

// ── Shared test schema ──────────────────────────────────────────────

const ProductSchema = Schema({
  fields: {
    id: Field.INT(),
    name: Field.STRING(),
    price: Field.DOUBLE(),
    internal_id: Field.STRING(),
    debug_flag: Field.BOOLEAN(),
  },
  primaryKey: { columns: ['id'] },
});

// ── 6.1: Snapshot tests for each component (reverse-nesting) ────────

describe('Rename (reverse-nesting)', () => {
  it('generates aliased SELECT for renamed fields', () => {
    const pipeline = Pipeline({
      name: 'rename-pipeline',
      children: [
        KafkaSink({
          topic: 'output',
          children: [
            Rename({
              columns: { id: 'product_id' },
              children: [
                KafkaSource({
                  topic: 'products',
                  schema: ProductSchema,
                  bootstrapServers: 'kafka:9092',
                }),
              ],
            }),
          ],
        }),
      ],
    });

    const result = generateSql(pipeline);
    expect(result.sql).toMatchSnapshot();
  });
});

describe('Drop (reverse-nesting)', () => {
  it('generates SELECT excluding dropped fields', () => {
    const pipeline = Pipeline({
      name: 'drop-pipeline',
      children: [
        KafkaSink({
          topic: 'output',
          children: [
            Drop({
              columns: ['internal_id', 'debug_flag'],
              children: [
                KafkaSource({
                  topic: 'products',
                  schema: ProductSchema,
                  bootstrapServers: 'kafka:9092',
                }),
              ],
            }),
          ],
        }),
      ],
    });

    const result = generateSql(pipeline);
    expect(result.sql).toMatchSnapshot();
  });
});

describe('Cast (reverse-nesting)', () => {
  it('generates CAST for targeted fields', () => {
    const pipeline = Pipeline({
      name: 'cast-pipeline',
      children: [
        KafkaSink({
          topic: 'output',
          children: [
            Cast({
              columns: { id: 'BIGINT', price: 'DECIMAL(10, 2)' },
              children: [
                KafkaSource({
                  topic: 'products',
                  schema: ProductSchema,
                  bootstrapServers: 'kafka:9092',
                }),
              ],
            }),
          ],
        }),
      ],
    });

    const result = generateSql(pipeline);
    expect(result.sql).toMatchSnapshot();
  });

  it('generates TRY_CAST when safe=true', () => {
    const pipeline = Pipeline({
      name: 'safe-cast-pipeline',
      children: [
        KafkaSink({
          topic: 'output',
          children: [
            Cast({
              columns: { id: 'BIGINT' },
              safe: true,
              children: [
                KafkaSource({
                  topic: 'products',
                  schema: ProductSchema,
                  bootstrapServers: 'kafka:9092',
                }),
              ],
            }),
          ],
        }),
      ],
    });

    const result = generateSql(pipeline);
    expect(result.sql).toMatchSnapshot();
  });
});

describe('Coalesce (reverse-nesting)', () => {
  it('generates COALESCE for targeted fields', () => {
    const pipeline = Pipeline({
      name: 'coalesce-pipeline',
      children: [
        KafkaSink({
          topic: 'output',
          children: [
            Coalesce({
              columns: { name: "'unknown'", price: '0' },
              children: [
                KafkaSource({
                  topic: 'products',
                  schema: ProductSchema,
                  bootstrapServers: 'kafka:9092',
                }),
              ],
            }),
          ],
        }),
      ],
    });

    const result = generateSql(pipeline);
    expect(result.sql).toMatchSnapshot();
  });
});

describe('AddField (reverse-nesting)', () => {
  it('generates SELECT *, expr AS alias', () => {
    const pipeline = Pipeline({
      name: 'addfield-pipeline',
      children: [
        KafkaSink({
          topic: 'output',
          children: [
            AddField({
              columns: {
                total: 'price * 2',
                processed_at: 'CURRENT_TIMESTAMP',
              },
              children: [
                KafkaSource({
                  topic: 'products',
                  schema: ProductSchema,
                  bootstrapServers: 'kafka:9092',
                }),
              ],
            }),
          ],
        }),
      ],
    });

    const result = generateSql(pipeline);
    expect(result.sql).toMatchSnapshot();
  });

  it('throws on name collision with upstream field', () => {
    expect(() => {
      const pipeline = Pipeline({
        name: 'collision-pipeline',
        children: [
          KafkaSink({
            topic: 'output',
            children: [
              AddField({
                columns: { id: 'new_value' },
                children: [
                  KafkaSource({
                    topic: 'products',
                    schema: ProductSchema,
                    bootstrapServers: 'kafka:9092',
                  }),
                ],
              }),
            ],
          }),
        ],
      });

      generateSql(pipeline);
    }).toThrow(/AddField name collision.*id/);
  });
});

// ── 6.2: Forward-reading JSX pattern ────────────────────────────────

describe('field transforms (forward-reading JSX)', () => {
  it('Rename works as a sibling between source and sink', () => {
    const pipeline = Pipeline({
      name: 'forward-rename',
      children: [
        KafkaSource({
          topic: 'products',
          schema: ProductSchema,
          bootstrapServers: 'kafka:9092',
        }),
        Rename({ columns: { id: 'product_id', name: 'product_name' } }),
        KafkaSink({ topic: 'output' }),
      ],
    });

    const result = generateSql(pipeline);
    expect(result.sql).toMatchSnapshot();
  });

  it('Drop works as a sibling between source and sink', () => {
    const pipeline = Pipeline({
      name: 'forward-drop',
      children: [
        KafkaSource({
          topic: 'products',
          schema: ProductSchema,
          bootstrapServers: 'kafka:9092',
        }),
        Drop({ columns: ['internal_id', 'debug_flag'] }),
        KafkaSink({ topic: 'output' }),
      ],
    });

    const result = generateSql(pipeline);
    expect(result.sql).toMatchSnapshot();
  });
});

// ── 6.3: Chained field transforms pipeline ──────────────────────────

describe('chained field transforms', () => {
  it('Drop → Rename → Cast → AddField produces nested SQL', () => {
    const pipeline = Pipeline({
      name: 'chained-pipeline',
      children: [
        KafkaSink({
          topic: 'output',
          children: [
            AddField({
              columns: { discounted: 'price * 0.9' },
              children: [
                Cast({
                  columns: { id: 'BIGINT' },
                  children: [
                    Rename({
                      columns: { name: 'product_name' },
                      children: [
                        Drop({
                          columns: ['debug_flag'],
                          children: [
                            KafkaSource({
                              topic: 'products',
                              schema: ProductSchema,
                              bootstrapServers: 'kafka:9092',
                            }),
                          ],
                        }),
                      ],
                    }),
                  ],
                }),
              ],
            }),
          ],
        }),
      ],
    });

    const result = generateSql(pipeline);
    expect(result.sql).toMatchSnapshot();
  });

  it('chained field transforms in forward-reading JSX pattern', () => {
    const pipeline = Pipeline({
      name: 'forward-chained',
      children: [
        KafkaSource({
          topic: 'products',
          schema: ProductSchema,
          bootstrapServers: 'kafka:9092',
        }),
        Drop({ columns: ['debug_flag'] }),
        Rename({ columns: { name: 'product_name' } }),
        Cast({ columns: { id: 'BIGINT' } }),
        AddField({ columns: { discounted: 'price * 0.9' } }),
        KafkaSink({ topic: 'output' }),
      ],
    });

    const result = generateSql(pipeline);
    expect(result.sql).toMatchSnapshot();
  });
});

// ── 6.4: Schema introspection tests ────────────────────────────────

describe('schema introspection', () => {
  it('Rename changes field names in sink schema', () => {
    const pipeline = Pipeline({
      name: 'introspect-rename',
      children: [
        KafkaSink({
          topic: 'output',
          children: [
            Rename({
              columns: { id: 'product_id' },
              children: [
                KafkaSource({
                  topic: 'products',
                  schema: ProductSchema,
                }),
              ],
            }),
          ],
        }),
      ],
    });

    const schemas = introspectPipelineSchemas(pipeline);
    const sinks = schemas.filter((s) => s.kind === 'sink');
    expect(sinks[0].columns).toHaveLength(5);
    expect(sinks[0].columns[0].name).toBe('product_id');
    expect(sinks[0].columns[0].type).toBe('INT');
    expect(sinks[0].columns[1].name).toBe('name');
  });

  it('Drop removes fields from sink schema', () => {
    const pipeline = Pipeline({
      name: 'introspect-drop',
      children: [
        KafkaSink({
          topic: 'output',
          children: [
            Drop({
              columns: ['internal_id', 'debug_flag'],
              children: [
                KafkaSource({
                  topic: 'products',
                  schema: ProductSchema,
                }),
              ],
            }),
          ],
        }),
      ],
    });

    const schemas = introspectPipelineSchemas(pipeline);
    const sinks = schemas.filter((s) => s.kind === 'sink');
    expect(sinks[0].columns).toHaveLength(3);
    expect(sinks[0].columns.map((c) => c.name)).toEqual(['id', 'name', 'price']);
  });

  it('Cast changes field types in sink schema', () => {
    const pipeline = Pipeline({
      name: 'introspect-cast',
      children: [
        KafkaSink({
          topic: 'output',
          children: [
            Cast({
              columns: { id: 'BIGINT', price: 'DECIMAL(10, 2)' },
              children: [
                KafkaSource({
                  topic: 'products',
                  schema: ProductSchema,
                }),
              ],
            }),
          ],
        }),
      ],
    });

    const schemas = introspectPipelineSchemas(pipeline);
    const sinks = schemas.filter((s) => s.kind === 'sink');
    expect(sinks[0].columns).toHaveLength(5);
    const idCol = sinks[0].columns.find((c) => c.name === 'id')!;
    expect(idCol.type).toBe('BIGINT');
    const priceCol = sinks[0].columns.find((c) => c.name === 'price')!;
    expect(priceCol.type).toBe('DECIMAL(10, 2)');
    // Unchanged fields keep original types
    const nameCol = sinks[0].columns.find((c) => c.name === 'name')!;
    expect(nameCol.type).toBe('STRING');
  });

  it('Coalesce preserves sink schema unchanged', () => {
    const pipeline = Pipeline({
      name: 'introspect-coalesce',
      children: [
        KafkaSink({
          topic: 'output',
          children: [
            Coalesce({
              columns: { name: "'unknown'" },
              children: [
                KafkaSource({
                  topic: 'products',
                  schema: ProductSchema,
                }),
              ],
            }),
          ],
        }),
      ],
    });

    const schemas = introspectPipelineSchemas(pipeline);
    const sinks = schemas.filter((s) => s.kind === 'sink');
    expect(sinks[0].columns).toHaveLength(5);
    // All types preserved
    expect(sinks[0].columns[0]).toEqual({ name: 'id', type: 'INT', constraints: [] });
    expect(sinks[0].columns[1]).toEqual({ name: 'name', type: 'STRING', constraints: [] });
  });

  it('AddField appends new fields to sink schema', () => {
    const pipeline = Pipeline({
      name: 'introspect-addfield',
      children: [
        KafkaSink({
          topic: 'output',
          children: [
            AddField({
              columns: { total: 'price * 2', tag: "'product'" },
              types: { total: 'DOUBLE' },
              children: [
                KafkaSource({
                  topic: 'products',
                  schema: ProductSchema,
                }),
              ],
            }),
          ],
        }),
      ],
    });

    const schemas = introspectPipelineSchemas(pipeline);
    const sinks = schemas.filter((s) => s.kind === 'sink');
    expect(sinks[0].columns).toHaveLength(7); // 5 original + 2 added
    expect(sinks[0].columns[5]).toEqual({ name: 'total', type: 'DOUBLE', constraints: [] });
    expect(sinks[0].columns[6]).toEqual({ name: 'tag', type: 'STRING', constraints: [] });
  });
});

// ── 6.5: PK propagation — rename a PK column ───────────────────────

describe('PK propagation', () => {
  it('Rename remaps primary key column names for upsert-kafka', () => {
    const pipeline = Pipeline({
      name: 'pk-rename',
      children: [
        KafkaSink({
          topic: 'output',
          children: [
            Rename({
              columns: { id: 'product_id' },
              children: [
                KafkaSource({
                  topic: 'products',
                  schema: ProductSchema,
                  format: 'debezium-json',
                  bootstrapServers: 'kafka:9092',
                  primaryKey: ['id'],
                }),
              ],
            }),
          ],
        }),
      ],
    });

    const result = generateSql(pipeline);
    // Should use upsert-kafka with renamed PK
    expect(result.sql).toContain('upsert-kafka');
    expect(result.sql).toContain('PRIMARY KEY (`product_id`) NOT ENFORCED');
  });

  // ── 6.6: PK propagation — drop a PK column ─────────────────────

  it('Drop clears primary key when PK column is dropped', () => {
    const pipeline = Pipeline({
      name: 'pk-drop',
      children: [
        KafkaSink({
          topic: 'output',
          children: [
            Drop({
              columns: ['id'],
              children: [
                KafkaSource({
                  topic: 'products',
                  schema: ProductSchema,
                  format: 'debezium-json',
                  bootstrapServers: 'kafka:9092',
                  primaryKey: ['id'],
                }),
              ],
            }),
          ],
        }),
      ],
    });

    const result = generateSql(pipeline);
    // Should NOT use upsert-kafka since PK was dropped
    expect(result.sql).not.toContain('upsert-kafka');
    expect(result.sql).toContain("'connector' = 'kafka'");
  });
});
