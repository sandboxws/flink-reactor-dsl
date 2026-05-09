// ── FlinkReactor browser-safe API ───────────────────────────────────
// This entry point re-exports all DSL primitives that are safe to use
// in a browser environment. Node-specific layers and modules are
// intentionally excluded.

// Codegen: YAML serializer (pure, no I/O — safe for browser)
export { toYaml } from "./codegen/crd-generator.js"
export type {
  SqlFragment,
  SqlSection,
  StatementMeta,
  StatementOrigin,
} from "./codegen/sql/sql-generator.js"
export { verifySql } from "./codegen/sql/sql-verifier.js"
// Components: catalog source
export type { CatalogSourceProps } from "./components/catalog-source.js"
export { CatalogSource } from "./components/catalog-source.js"
export type {
  CatalogHandle,
  CatalogResult,
  GenericCatalogProps,
  HiveCatalogProps,
  IcebergCatalogProps,
  IcebergCatalogType,
  JdbcCatalogProps,
  PaimonCatalogProps,
} from "./components/catalogs.js"
// Components: catalogs
export {
  GenericCatalog,
  HiveCatalog,
  IcebergCatalog,
  JdbcCatalog,
  PaimonCatalog,
} from "./components/catalogs.js"
export type {
  MatchAfterStrategy,
  MatchRecognizeProps,
} from "./components/cep.js"
// Components: CEP
export { MatchRecognize } from "./components/cep.js"
export type { RawSQLProps, UDFProps } from "./components/escape-hatches.js"
// Components: escape hatches
export { RawSQL, UDF } from "./components/escape-hatches.js"
export type {
  AddFieldProps,
  CastProps,
  CoalesceProps,
  DropProps,
  RenameProps,
} from "./components/field-transforms.js"
// Components: field transforms
export {
  AddField,
  Cast,
  Coalesce,
  Drop,
  Rename,
} from "./components/field-transforms.js"
export type {
  IntervalBounds,
  IntervalJoinProps,
  JoinHints,
  JoinProps,
  JoinType,
  LookupJoinProps,
  TemporalJoinProps,
} from "./components/joins.js"
// Components: joins
export {
  IntervalJoin,
  Join,
  LookupJoin,
  TemporalJoin,
} from "./components/joins.js"
export type { LateralJoinProps } from "./components/lateral-join.js"
export { LateralJoin } from "./components/lateral-join.js"
export type { MaterializedTableProps } from "./components/materialized-table.js"
// Components: materialized table
export { MaterializedTable } from "./components/materialized-table.js"
export type {
  CheckpointConfig,
  PipelineMode,
  PipelineProps,
  RestartStrategy,
  StateBackend,
} from "./components/pipeline.js"
// Components: pipeline
export { Pipeline } from "./components/pipeline.js"
export type { QualifyProps } from "./components/qualify.js"
export { Qualify } from "./components/qualify.js"
export type {
  ColumnExpr,
  QueryGroupByProps,
  QueryHavingProps,
  QueryOrderByProps,
  QueryProps,
  QuerySelectProps,
  QueryWhereProps,
  WindowFunctionExpr,
  WindowSpec,
} from "./components/query.js"
export { Query } from "./components/query.js"
export type {
  RouteBranchProps,
  RouteDefaultProps,
  RouteProps,
} from "./components/route.js"
// Components: route
export { Route } from "./components/route.js"
export type {
  SideOutputProps,
  SideOutputSinkProps,
} from "./components/side-output.js"
export { SideOutput } from "./components/side-output.js"
export type {
  FileFormat,
  FileSystemSinkProps,
  GenericSinkProps,
  IcebergParquetCompression,
  IcebergSinkProps,
  IcebergWriteDistributionMode,
  JdbcSinkProps,
  KafkaSinkProps,
  PaimonChangelogProducer,
  PaimonMergeEngine,
  PaimonSinkProps,
  RollingPolicy,
  SinkFormat,
} from "./components/sinks.js"
// Components: sinks
export {
  FileSystemSink,
  GenericSink,
  IcebergSink,
  JdbcSink,
  KafkaSink,
  PaimonSink,
} from "./components/sinks.js"
export type {
  GenericSourceProps,
  JdbcSourceProps,
  KafkaFormat,
  KafkaSourceProps,
  KafkaStartupMode,
} from "./components/sources.js"
// Components: sources
export { GenericSource, JdbcSource, KafkaSource } from "./components/sources.js"
export type {
  AggregateProps,
  DeduplicateProps,
  FilterProps,
  FlatMapProps,
  MapProps,
  TopNProps,
  UnionProps,
} from "./components/transforms.js"
// Components: transforms
export {
  Aggregate,
  Deduplicate,
  Filter,
  FlatMap,
  Map,
  TopN,
  Union,
} from "./components/transforms.js"
export type {
  ValidateProps,
  ValidateRejectProps,
  ValidationRules,
} from "./components/validate.js"
export { Validate } from "./components/validate.js"
export type { ViewProps } from "./components/view.js"
// Components: view, query, side-output, lateral-join, validate
export { View } from "./components/view.js"
export type {
  SessionWindowProps,
  SlideWindowProps,
  TumbleWindowProps,
} from "./components/windows.js"
// Components: windows
export {
  SessionWindow,
  SlideWindow,
  TumbleWindow,
} from "./components/windows.js"
export type {
  AppSynthResult,
  FlinkReactorAppProps,
  PipelineArtifact,
} from "./core/app.js"
// Core: app
export { synthesizeApp } from "./core/app.js"
export { validateChangelogModes } from "./core/changelog-propagation.js"
export type {
  ClusterConfig,
  ConnectorConfig,
  DashboardAuthConfig,
  DashboardObservabilityConfig,
  DashboardRbacConfig,
  DashboardSection,
  DashboardSslConfig,
  DeliveryStrategy,
  EnvironmentEntry,
  FlinkReactorConfig,
  InfraConfig,
  PipelineOverrides,
} from "./core/config.js"
// Core: config
export { defineConfig } from "./core/config.js"
export { validateConnectorProperties } from "./core/connector-validation.js"
// NOTE: resolveConfig and toInfraConfigFromResolved are excluded (Node-dependent)
// NOTE: discoverEnvironments is excluded (Node-dependent via node:fs)
// Core: Effect foundation — bridge utilities
export {
  fromThrowable,
  fromThrowableAsync,
  runPromise,
  runSync,
  runWithCause,
  toValidationEffect,
} from "./core/effect-utils.js"
export type { EnvVarRef, Resolved } from "./core/env-var.js"
export { env, isEnvVarRef, resolveEnvVars } from "./core/env-var.js"
export type { EnvironmentConfig } from "./core/environment.js"
export { defineEnvironment, resolveEnvironment } from "./core/environment.js"
export type { SqlGatewayError, SynthError } from "./core/errors.js"
// Core: Effect foundation — errors
export {
  CliError,
  ClusterError,
  ConfigError,
  CrdGenerationError,
  CycleDetectedError,
  DiscoveryError,
  FileSystemError,
  PluginError,
  SchemaError,
  SqlGatewayConnectionError,
  SqlGatewayResponseError,
  SqlGatewayTimeoutError,
  SqlGenerationError,
  ValidationError,
} from "./core/errors.js"
export type {
  FeatureGateError,
  JdbcConnectorInfo,
} from "./core/flink-compat.js"
// Core: Flink version compat
export { FlinkVersionCompat } from "./core/flink-compat.js"
// Core: JSX runtime
export {
  createElement,
  Fragment,
  jsx,
  jsxs,
  resetNodeIdCounter,
} from "./core/jsx-runtime.js"
// NOTE: Node-specific layers are excluded:
// ConfigProviderLive, NodeFileSystemLive, PipelineLoaderLive,
// ProcessRunnerLive, CliOutputLive, NodeHttpClientLive, MainLive
// Core: plugins
export type {
  AfterSynthHookContext,
  FlinkReactorPlugin,
  PipelineSynthHookResult,
  PluginDdlGenerator,
  PluginSqlGenerator,
  PluginValidator,
  SynthHookContext,
} from "./core/plugin.js"
export type { ResolvedPluginChain } from "./core/plugin-registry.js"
export { EMPTY_PLUGIN_CHAIN, resolvePlugins } from "./core/plugin-registry.js"
export type {
  MetadataColumnDeclaration,
  PrimaryKeyDeclaration,
  SchemaDefinition,
  SchemaOptions,
  WatermarkDeclaration,
} from "./core/schema.js"
// Core: schema
export { Field, isValidFlinkType, Schema } from "./core/schema.js"
// Core: validation — individual validators for sandbox integration
export {
  validateExpressionSyntax,
  validateSchemaReferences,
} from "./core/schema-validation.js"
export type {
  CliOutputService,
  ConfigProviderService,
  FrFileSystemService,
  FrHttpClientService,
  HttpResponse,
  PipelineLoaderService,
  ProcessResult,
  ProcessRunnerService,
} from "./core/services.js"
// Core: Effect foundation — services (service interfaces, not Node implementations)
export {
  CliOutput,
  ConfigProvider,
  FrFileSystem,
  FrHttpClient,
  PipelineLoader,
  ProcessRunner,
} from "./core/services.js"
export type { GraphEdge, ValidationDiagnostic } from "./core/synth-context.js"
// Core: synth context
export { SynthContext } from "./core/synth-context.js"
// Core: tree utilities
export {
  findNodes,
  mapTree,
  rekindTree,
  replaceChild,
  walkTree,
  wrapNode,
} from "./core/tree-utils.js"
// Core: types
export type {
  BaseComponentProps,
  ChangelogMode,
  ConstructNode,
  FlinkCompositeType,
  FlinkMajorVersion,
  FlinkParameterizedType,
  FlinkPrimitiveType,
  FlinkSchema,
  FlinkType,
  NodeKind,
  Stream,
  TypedConstructNode,
} from "./core/types.js"
export { createStream } from "./core/types.js"
