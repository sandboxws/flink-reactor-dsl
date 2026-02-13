import { describe, it, expect } from 'vitest';
import ts from 'typescript';
import { getNestingDiagnostics } from '../src/diagnostics';
import { createRulesRegistry } from '../src/component-rules';

function parse(source: string): ts.SourceFile {
  return ts.createSourceFile(
    'test.tsx',
    source,
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.TSX,
  );
}

describe('getNestingDiagnostics', () => {
  const registry = createRulesRegistry();

  it('reports no diagnostics for valid nesting', () => {
    const sf = parse('<Pipeline><KafkaSource /></Pipeline>');
    const diags = getNestingDiagnostics(sf, registry, ts);
    expect(diags).toHaveLength(0);
  });

  it('reports no diagnostics for valid Route children', () => {
    const sf = parse('<Route><Route.Branch></Route.Branch></Route>');
    const diags = getNestingDiagnostics(sf, registry, ts);
    expect(diags).toHaveLength(0);
  });

  it('reports warning for Filter inside Route', () => {
    const sf = parse('<Route><Filter /></Route>');
    const diags = getNestingDiagnostics(sf, registry, ts);
    expect(diags).toHaveLength(1);
    expect(diags[0].category).toBe(ts.DiagnosticCategory.Warning);
    expect(diags[0].messageText).toContain("'Filter' is not a valid child of 'Route'");
    expect(diags[0].messageText).toContain('Route.Branch');
    expect(diags[0].messageText).toContain('Route.Default');
  });

  it('reports warning for KafkaSink inside Route', () => {
    const sf = parse('<Route><KafkaSink /></Route>');
    const diags = getNestingDiagnostics(sf, registry, ts);
    expect(diags).toHaveLength(1);
    expect(diags[0].messageText).toContain("'KafkaSink' is not a valid child of 'Route'");
  });

  it('reports no diagnostics for wildcard parents', () => {
    const sf = parse('<Route.Branch><Filter /></Route.Branch>');
    const diags = getNestingDiagnostics(sf, registry, ts);
    expect(diags).toHaveLength(0);
  });

  it('reports no diagnostics for unrecognized parents', () => {
    const sf = parse('<MyWrapper><Anything /></MyWrapper>');
    const diags = getNestingDiagnostics(sf, registry, ts);
    expect(diags).toHaveLength(0);
  });

  it('diagnostic span points to the child tag name', () => {
    const source = '<Route><Filter /></Route>';
    const sf = parse(source);
    const diags = getNestingDiagnostics(sf, registry, ts);
    expect(diags).toHaveLength(1);
    const spanText = source.slice(diags[0].start!, diags[0].start! + diags[0].length!);
    expect(spanText).toBe('Filter');
  });

  it('checks multiple children', () => {
    const sf = parse(
      '<Route><Filter /><Route.Branch></Route.Branch><KafkaSink /></Route>',
    );
    const diags = getNestingDiagnostics(sf, registry, ts);
    expect(diags).toHaveLength(2);
    expect(diags[0].messageText).toContain("'Filter'");
    expect(diags[1].messageText).toContain("'KafkaSink'");
  });

  it('checks nested structures independently', () => {
    // Filter inside Pipeline is fine, but Filter inside Route is invalid
    const sf = parse(
      '<Pipeline><Filter /><Route><Filter /></Route></Pipeline>',
    );
    const diags = getNestingDiagnostics(sf, registry, ts);
    expect(diags).toHaveLength(1);
    expect(diags[0].messageText).toContain("'Filter' is not a valid child of 'Route'");
  });

  it('uses Warning severity', () => {
    const sf = parse('<Route><Filter /></Route>');
    const diags = getNestingDiagnostics(sf, registry, ts);
    expect(diags[0].category).toBe(ts.DiagnosticCategory.Warning);
  });

  it('includes source identifier', () => {
    const sf = parse('<Route><Filter /></Route>');
    const diags = getNestingDiagnostics(sf, registry, ts);
    expect(diags[0].source).toBe('flink-reactor');
  });

  it('validates Query children', () => {
    const sf = parse(
      '<Query><Query.Select></Query.Select><Filter /></Query>',
    );
    const diags = getNestingDiagnostics(sf, registry, ts);
    expect(diags).toHaveLength(1);
    expect(diags[0].messageText).toContain("'Filter' is not a valid child of 'Query'");
  });
});
