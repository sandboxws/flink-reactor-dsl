/**
 * Component hierarchy registry for flink-reactor JSX components.
 *
 * Maps parent component names to their valid children. Used by the
 * completion filter and diagnostics modules to provide context-aware
 * IDE behavior.
 */

/** Built-in parent → allowed children mapping */
const COMPONENT_CHILDREN: Record<string, string[] | '*'> = {
  Pipeline: [
    'KafkaSource', 'JdbcSource', 'GenericSource', 'CatalogSource',
    'Filter', 'Map', 'FlatMap', 'Aggregate', 'Union', 'Deduplicate', 'TopN',
    'Route', 'Join', 'TemporalJoin', 'LookupJoin', 'IntervalJoin', 'LateralJoin',
    'TumbleWindow', 'SlideWindow', 'SessionWindow',
    'KafkaSink', 'JdbcSink', 'FileSystemSink', 'PaimonSink', 'IcebergSink', 'GenericSink',
    'Query', 'RawSQL', 'UDF', 'MatchRecognize', 'SideOutput', 'Validate', 'View',
  ],
  Route: ['Route.Branch', 'Route.Default'],
  'Route.Branch': '*',
  'Route.Default': '*',
  Query: [
    'Query.Select', 'Query.Where', 'Query.GroupBy', 'Query.Having', 'Query.OrderBy',
    'KafkaSource', 'JdbcSource', 'GenericSource', 'CatalogSource',
  ],
  SideOutput: [
    'SideOutput.Sink',
    'KafkaSource', 'JdbcSource', 'GenericSource', 'CatalogSource',
    'Filter', 'Map', 'FlatMap',
  ],
  Validate: [
    'Validate.Reject',
    'KafkaSource', 'JdbcSource', 'GenericSource', 'CatalogSource',
    'Filter', 'Map', 'FlatMap',
  ],
  'SideOutput.Sink': '*',
  'Validate.Reject': '*',
};

export interface ComponentRulesRegistry {
  /** Get allowed children for a parent, or undefined if parent is unrecognized */
  getAllowedChildren(parent: string): string[] | '*' | undefined;
  /** Check if a child is valid for a parent */
  isValidChild(parent: string, child: string): boolean;
}

/** Merge built-in rules with user config overrides */
export function createRulesRegistry(
  userRules?: Record<string, string[] | '*'>,
): ComponentRulesRegistry {
  const rules: Record<string, string[] | '*'> = {
    ...COMPONENT_CHILDREN,
    ...userRules,
  };

  return {
    getAllowedChildren(parent: string): string[] | '*' | undefined {
      return rules[parent];
    },

    isValidChild(parent: string, child: string): boolean {
      const allowed = rules[parent];
      if (allowed === undefined) return true; // unknown parent → allow all
      if (allowed === '*') return true;
      return allowed.includes(child);
    },
  };
}
