// ── FlinkReactor public API ──────────────────────────────────────────
// This is the library entry point for `import { ... } from 'flink-reactor'`.

// Core: config & environment
export { defineConfig } from './core/config.js';
export type { FlinkReactorConfig, InfraConfig, ConnectorConfig, DeliveryStrategy } from './core/config.js';
export { defineEnvironment, resolveEnvironment, discoverEnvironments } from './core/environment.js';
export type { EnvironmentConfig, PipelineOverrides } from './core/environment.js';

// Core: types
export type {
  FlinkType,
  FlinkPrimitiveType,
  FlinkParameterizedType,
  FlinkCompositeType,
  FlinkMajorVersion,
  FlinkSchema,
  Stream,
  ChangelogMode,
  BaseComponentProps,
  NodeKind,
  ConstructNode,
  TypedConstructNode,
} from './core/types.js';
export { createStream } from './core/types.js';

// Core: schema
export { Schema, Field, isValidFlinkType } from './core/schema.js';
export type { SchemaDefinition, SchemaOptions, WatermarkDeclaration, PrimaryKeyDeclaration, MetadataColumnDeclaration } from './core/schema.js';

// Core: JSX runtime
export { createElement, Fragment, jsx, jsxs } from './core/jsx-runtime.js';

// Core: synth context
export { SynthContext } from './core/synth-context.js';
export type { GraphEdge, ValidationDiagnostic } from './core/synth-context.js';

// Core: app
export { synthesizeApp } from './core/app.js';
export type { FlinkReactorAppProps, PipelineArtifact, AppSynthResult } from './core/app.js';

// Core: Flink version compat
export { FlinkVersionCompat } from './core/flink-compat.js';
export type { JdbcConnectorInfo, FeatureGateError } from './core/flink-compat.js';

// Core: plugins
export type { FlinkReactorPlugin, SynthHookContext, PipelineSynthHookResult, AfterSynthHookContext, PluginSqlGenerator, PluginDdlGenerator, PluginValidator } from './core/plugin.js';
export { resolvePlugins, EMPTY_PLUGIN_CHAIN } from './core/plugin-registry.js';
export type { ResolvedPluginChain } from './core/plugin-registry.js';

// Core: tree utilities
export { rekindTree, mapTree, walkTree, findNodes, wrapNode, replaceChild } from './core/tree-utils.js';

// Components: pipeline
export { Pipeline } from './components/pipeline.js';
export type { PipelineProps, PipelineMode, StateBackend, CheckpointConfig, RestartStrategy } from './components/pipeline.js';

// Components: sources
export { KafkaSource, JdbcSource, GenericSource } from './components/sources.js';
export type { KafkaSourceProps, JdbcSourceProps, GenericSourceProps, KafkaFormat, KafkaStartupMode } from './components/sources.js';

// Components: sinks
export { KafkaSink, JdbcSink, FileSystemSink, GenericSink, PaimonSink, IcebergSink } from './components/sinks.js';
export type { KafkaSinkProps, JdbcSinkProps, FileSystemSinkProps, GenericSinkProps, PaimonSinkProps, IcebergSinkProps, SinkFormat, FileFormat, RollingPolicy, PaimonMergeEngine, PaimonChangelogProducer } from './components/sinks.js';

// Components: transforms
export { Filter, Map, FlatMap, Aggregate, Union, Deduplicate, TopN } from './components/transforms.js';
export type { FilterProps, MapProps, FlatMapProps, AggregateProps, UnionProps, DeduplicateProps, TopNProps } from './components/transforms.js';

// Components: field transforms
export { Rename, Drop, Cast, Coalesce, AddField } from './components/field-transforms.js';
export type { RenameProps, DropProps, CastProps, CoalesceProps, AddFieldProps } from './components/field-transforms.js';

// Components: route
export { Route } from './components/route.js';
export type { RouteProps, RouteBranchProps, RouteDefaultProps } from './components/route.js';

// Components: joins
export { Join, TemporalJoin, LookupJoin, IntervalJoin } from './components/joins.js';
export type { JoinProps, TemporalJoinProps, LookupJoinProps, IntervalJoinProps, JoinType, JoinHints, IntervalBounds } from './components/joins.js';

// Components: windows
export { TumbleWindow, SlideWindow, SessionWindow } from './components/windows.js';
export type { TumbleWindowProps, SlideWindowProps, SessionWindowProps } from './components/windows.js';

// Components: catalogs
export { PaimonCatalog, IcebergCatalog, HiveCatalog, JdbcCatalog, GenericCatalog } from './components/catalogs.js';
export type { PaimonCatalogProps, IcebergCatalogProps, HiveCatalogProps, JdbcCatalogProps, GenericCatalogProps, CatalogHandle, CatalogResult, IcebergCatalogType } from './components/catalogs.js';

// Components: catalog source
export { CatalogSource } from './components/catalog-source.js';
export type { CatalogSourceProps } from './components/catalog-source.js';

// Components: escape hatches
export { RawSQL, UDF } from './components/escape-hatches.js';
export type { RawSQLProps, UDFProps } from './components/escape-hatches.js';

// Components: CEP
export { MatchRecognize } from './components/cep.js';
export type { MatchRecognizeProps, MatchAfterStrategy } from './components/cep.js';

// Components: view, query, side-output, lateral-join, validate
export { View } from './components/view.js';
export type { ViewProps } from './components/view.js';
export { Query } from './components/query.js';
export type { QueryProps, QuerySelectProps, QueryWhereProps, QueryGroupByProps, QueryHavingProps, QueryOrderByProps, WindowSpec, WindowFunctionExpr, ColumnExpr } from './components/query.js';
export { SideOutput } from './components/side-output.js';
export type { SideOutputProps, SideOutputSinkProps } from './components/side-output.js';
export { LateralJoin } from './components/lateral-join.js';
export type { LateralJoinProps } from './components/lateral-join.js';
export { Validate } from './components/validate.js';
export type { ValidateProps, ValidateRejectProps, ValidationRules } from './components/validate.js';
