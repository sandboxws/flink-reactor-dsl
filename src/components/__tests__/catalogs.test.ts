import { describe, it, expect, beforeEach } from 'vitest';
import { resetNodeIdCounter } from '../../core/jsx-runtime.js';
import {
  PaimonCatalog,
  IcebergCatalog,
  HiveCatalog,
  JdbcCatalog,
  GenericCatalog,
} from '../catalogs.js';

beforeEach(() => {
  resetNodeIdCounter();
});

describe('PaimonCatalog', () => {
  it('creates a Catalog node with name and warehouse', () => {
    const { node, handle } = PaimonCatalog({
      name: 'lake',
      warehouse: 's3://bucket/paimon',
    });

    expect(node.kind).toBe('Catalog');
    expect(node.component).toBe('PaimonCatalog');
    expect(node.props.name).toBe('lake');
    expect(node.props.warehouse).toBe('s3://bucket/paimon');
    expect(handle._tag).toBe('CatalogHandle');
    expect(handle.catalogName).toBe('lake');
    expect(handle.nodeId).toBe(node.id);
  });

  it('stores optional metastore prop', () => {
    const { node } = PaimonCatalog({
      name: 'lake',
      warehouse: 's3://bucket/paimon',
      metastore: 'hive',
    });

    expect(node.props.metastore).toBe('hive');
  });
});

describe('IcebergCatalog', () => {
  it('creates a Catalog node with catalogType and uri', () => {
    const { node, handle } = IcebergCatalog({
      name: 'iceberg_cat',
      catalogType: 'rest',
      uri: 'http://iceberg-rest:8181',
    });

    expect(node.kind).toBe('Catalog');
    expect(node.component).toBe('IcebergCatalog');
    expect(node.props.catalogType).toBe('rest');
    expect(node.props.uri).toBe('http://iceberg-rest:8181');
    expect(handle.catalogName).toBe('iceberg_cat');
  });
});

describe('HiveCatalog', () => {
  it('creates a Catalog node with hiveConfDir', () => {
    const { node, handle } = HiveCatalog({
      name: 'hive',
      hiveConfDir: '/etc/hive/conf',
    });

    expect(node.kind).toBe('Catalog');
    expect(node.component).toBe('HiveCatalog');
    expect(node.props.hiveConfDir).toBe('/etc/hive/conf');
    expect(handle.catalogName).toBe('hive');
  });
});

describe('JdbcCatalog', () => {
  it('creates a Catalog node with baseUrl and defaultDatabase', () => {
    const { node, handle } = JdbcCatalog({
      name: 'pg_catalog',
      baseUrl: 'jdbc:postgresql://localhost:5432/',
      defaultDatabase: 'analytics',
    });

    expect(node.kind).toBe('Catalog');
    expect(node.component).toBe('JdbcCatalog');
    expect(node.props.baseUrl).toBe('jdbc:postgresql://localhost:5432/');
    expect(node.props.defaultDatabase).toBe('analytics');
    expect(handle.catalogName).toBe('pg_catalog');
  });
});

describe('GenericCatalog', () => {
  it('creates a Catalog node with type and options', () => {
    const { node, handle } = GenericCatalog({
      name: 'custom',
      type: 'custom-catalog',
      options: { 'catalog.endpoint': 'http://localhost:9090' },
    });

    expect(node.kind).toBe('Catalog');
    expect(node.component).toBe('GenericCatalog');
    expect(node.props.type).toBe('custom-catalog');
    expect(node.props.options).toEqual({
      'catalog.endpoint': 'http://localhost:9090',
    });
    expect(handle.catalogName).toBe('custom');
  });
});

describe('Catalog handle referencing', () => {
  it('produces a handle usable by downstream sinks/sources', () => {
    const { handle } = PaimonCatalog({
      name: 'lake',
      warehouse: 's3://bucket/paimon',
    });

    expect(handle._tag).toBe('CatalogHandle');
    expect(handle.catalogName).toBe('lake');
    expect(typeof handle.nodeId).toBe('string');
  });
});
