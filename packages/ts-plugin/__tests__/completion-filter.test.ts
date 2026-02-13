import { describe, it, expect } from 'vitest';
import type ts from 'typescript';
import { filterCompletions } from '../src/completion-filter';
import { createRulesRegistry } from '../src/component-rules';

/** Create a mock CompletionEntry */
function entry(name: string, kind: string = 'function'): ts.CompletionEntry {
  return {
    name,
    kind: kind as ts.ScriptElementKind,
    sortText: '0',
    kindModifiers: '',
  };
}

/** Create a mock CompletionInfo with the given entries */
function completionInfo(entries: ts.CompletionEntry[]): ts.CompletionInfo {
  return {
    isGlobalCompletion: false,
    isMemberCompletion: false,
    isNewIdentifierLocation: false,
    entries,
  };
}

describe('filterCompletions', () => {
  const registry = createRulesRegistry();

  it('passes through all completions when parentName is undefined', () => {
    const info = completionInfo([
      entry('KafkaSource'),
      entry('Filter'),
      entry('SomethingUnrelated'),
    ]);
    const result = filterCompletions(info, undefined, registry);
    expect(result.entries).toHaveLength(3);
  });

  it('passes through all completions for unknown parent', () => {
    const info = completionInfo([
      entry('KafkaSource'),
      entry('Filter'),
      entry('SomethingUnrelated'),
    ]);
    const result = filterCompletions(info, 'UnknownComponent', registry);
    expect(result.entries).toHaveLength(3);
  });

  it('passes through all completions for wildcard parent', () => {
    const info = completionInfo([
      entry('KafkaSource'),
      entry('Filter'),
      entry('SomethingUnrelated'),
    ]);
    const result = filterCompletions(info, 'Route.Branch', registry);
    expect(result.entries).toHaveLength(3);
  });

  it('filters Pipeline children to valid components only', () => {
    const info = completionInfo([
      entry('KafkaSource'),         // valid Pipeline child
      entry('Filter'),              // valid Pipeline child
      entry('Route.Branch'),        // NOT a valid Pipeline child
      entry('MuiIcon', 'class'),    // unrelated uppercase class
    ]);
    const result = filterCompletions(info, 'Pipeline', registry);
    const names = result.entries.map((e) => e.name);
    expect(names).toContain('KafkaSource');
    expect(names).toContain('Filter');
    expect(names).not.toContain('Route.Branch');
    expect(names).not.toContain('MuiIcon');
  });

  it('filters Route children to Route.Branch and Route.Default only', () => {
    const info = completionInfo([
      entry('Route.Branch'),
      entry('Route.Default'),
      entry('Filter'),
      entry('KafkaSource'),
    ]);
    const result = filterCompletions(info, 'Route', registry);
    const names = result.entries.map((e) => e.name);
    expect(names).toEqual(['Route.Branch', 'Route.Default']);
  });

  it('filters Query children correctly', () => {
    const info = completionInfo([
      entry('Query.Select'),
      entry('Query.Where'),
      entry('Filter'),         // not valid in Query
      entry('KafkaSource'),    // valid in Query
    ]);
    const result = filterCompletions(info, 'Query', registry);
    const names = result.entries.map((e) => e.name);
    expect(names).toContain('Query.Select');
    expect(names).toContain('Query.Where');
    expect(names).toContain('KafkaSource');
    expect(names).not.toContain('Filter');
  });

  it('preserves non-component completions (keywords, variables)', () => {
    const info = completionInfo([
      entry('KafkaSource'),
      entry('const', 'keyword'),          // keyword — preserved
      entry('myVariable', 'var'),         // lowercase var — preserved (not a component)
      entry('SomeIrrelevant', 'function'), // uppercase function — filtered
    ]);
    const result = filterCompletions(info, 'Route', registry);
    const names = result.entries.map((e) => e.name);
    expect(names).toContain('const');
    expect(names).toContain('myVariable');
    expect(names).not.toContain('SomeIrrelevant');
  });

  it('preserves lowercase entries even with component-like kinds', () => {
    const info = completionInfo([
      entry('useState', 'function'),   // lowercase function — not a component
      entry('Filter'),                 // uppercase — filtered by Route rules
    ]);
    const result = filterCompletions(info, 'Route', registry);
    const names = result.entries.map((e) => e.name);
    expect(names).toContain('useState');
    expect(names).not.toContain('Filter');
  });
});
