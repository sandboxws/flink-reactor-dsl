import { describe, it, expect } from 'vitest';
import ts from 'typescript';
import { findParentJsxComponent, getComponentName } from '../src/context-detector';

/** Parse a TSX string and return the source file */
function parse(source: string): ts.SourceFile {
  return ts.createSourceFile(
    'test.tsx',
    source,
    ts.ScriptTarget.Latest,
    /* setParentNodes */ true,
    ts.ScriptKind.TSX,
  );
}

/** Find the position of a cursor marker (|) in the source, then return source without the marker */
function withCursor(source: string): { text: string; position: number } {
  const position = source.indexOf('|');
  if (position === -1) throw new Error('No cursor marker (|) found in source');
  const text = source.slice(0, position) + source.slice(position + 1);
  return { text, position };
}

describe('getComponentName', () => {
  it('extracts simple identifier', () => {
    const sf = parse('<Pipeline />');
    const jsx = sf.statements[0] as ts.ExpressionStatement;
    const element = jsx.expression as ts.JsxSelfClosingElement;
    expect(getComponentName(element.tagName, ts)).toBe('Pipeline');
  });

  it('extracts dot-notation name', () => {
    const sf = parse('<Route.Branch />');
    const jsx = sf.statements[0] as ts.ExpressionStatement;
    const element = jsx.expression as ts.JsxSelfClosingElement;
    expect(getComponentName(element.tagName, ts)).toBe('Route.Branch');
  });
});

describe('findParentJsxComponent', () => {
  it('detects Pipeline as parent', () => {
    const { text, position } = withCursor('<Pipeline>|</Pipeline>');
    const sf = parse(text);
    expect(findParentJsxComponent(sf, position, ts)).toBe('Pipeline');
  });

  it('detects Route as parent (not Pipeline) for nested context', () => {
    const { text, position } = withCursor(
      '<Pipeline><Route>|</Route></Pipeline>',
    );
    const sf = parse(text);
    expect(findParentJsxComponent(sf, position, ts)).toBe('Route');
  });

  it('detects dot-notation parent', () => {
    const { text, position } = withCursor(
      '<Route.Branch>|</Route.Branch>',
    );
    const sf = parse(text);
    expect(findParentJsxComponent(sf, position, ts)).toBe('Route.Branch');
  });

  it('returns undefined at top level', () => {
    const { text, position } = withCursor('const x = 1;|');
    const sf = parse(text);
    expect(findParentJsxComponent(sf, position, ts)).toBeUndefined();
  });

  it('returns undefined inside self-closing element (no children context)', () => {
    // Cursor is inside the tag itself, not in a children position
    const { text, position } = withCursor('<Pipeline />|');
    const sf = parse(text);
    // Position is after the self-closing element, so no parent
    expect(findParentJsxComponent(sf, position, ts)).toBeUndefined();
  });

  it('detects parent when cursor is between children', () => {
    const { text, position } = withCursor(
      '<Pipeline><KafkaSource />\n|<Filter /></Pipeline>',
    );
    const sf = parse(text);
    expect(findParentJsxComponent(sf, position, ts)).toBe('Pipeline');
  });

  it('detects deeply nested parent', () => {
    const { text, position } = withCursor(
      '<Pipeline><Route><Route.Branch>|</Route.Branch></Route></Pipeline>',
    );
    const sf = parse(text);
    expect(findParentJsxComponent(sf, position, ts)).toBe('Route.Branch');
  });

  it('skips fragments and finds non-fragment parent', () => {
    const { text, position } = withCursor(
      '<Pipeline><>|</></Pipeline>',
    );
    const sf = parse(text);
    // Fragment is transparent, so Pipeline is the parent
    expect(findParentJsxComponent(sf, position, ts)).toBe('Pipeline');
  });

  it('returns undefined inside a standalone fragment', () => {
    const { text, position } = withCursor('<>|</>');
    const sf = parse(text);
    // No non-fragment parent
    expect(findParentJsxComponent(sf, position, ts)).toBeUndefined();
  });

  it('detects Query parent', () => {
    const { text, position } = withCursor(
      '<Query>|</Query>',
    );
    const sf = parse(text);
    expect(findParentJsxComponent(sf, position, ts)).toBe('Query');
  });
});
