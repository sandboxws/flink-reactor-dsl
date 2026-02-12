import { describe, it, expect } from 'vitest';
import { mapTree, walkTree, findNodes, wrapNode, replaceChild } from '../tree-utils.js';
import type { ConstructNode } from '../types.js';

function makeNode(id: string, kind: ConstructNode['kind'], children: ConstructNode[] = []): ConstructNode {
  return { id, kind, component: id, props: {}, children };
}

// Build a simple tree:
//   pipeline
//     └── sink
//           └── filter
//                 └── source
function makeSimpleTree(): ConstructNode {
  const source = makeNode('source', 'Source');
  const filter = makeNode('filter', 'Transform', [source]);
  const sink = makeNode('sink', 'Sink', [filter]);
  return makeNode('pipeline', 'Pipeline', [sink]);
}

describe('mapTree', () => {
  it('visits all nodes in post-order', () => {
    const tree = makeSimpleTree();
    const visited: string[] = [];

    mapTree(tree, (node) => {
      visited.push(node.id);
      return node;
    });

    // Post-order: source, filter, sink, pipeline
    expect(visited).toEqual(['source', 'filter', 'sink', 'pipeline']);
  });

  it('returns same reference when visitor is identity', () => {
    const tree = makeSimpleTree();
    const result = mapTree(tree, (node) => node);
    expect(result).toBe(tree);
  });

  it('creates new tree when visitor modifies a leaf', () => {
    const tree = makeSimpleTree();

    const result = mapTree(tree, (node) => {
      if (node.id === 'source') {
        return { ...node, props: { modified: true } };
      }
      return node;
    });

    // Root should be different (new object due to child change)
    expect(result).not.toBe(tree);
    // The modified source should have new props
    const source = result.children[0].children[0].children[0];
    expect(source.props.modified).toBe(true);
  });

  it('preserves unchanged subtrees via structural sharing', () => {
    const source = makeNode('source', 'Source');
    const filterA = makeNode('filterA', 'Transform', [source]);
    const filterB = makeNode('filterB', 'Transform');
    const pipeline = makeNode('pipeline', 'Pipeline', [filterA, filterB]);

    const result = mapTree(pipeline, (node) => {
      if (node.id === 'source') {
        return { ...node, props: { modified: true } };
      }
      return node;
    });

    // filterB was not modified, so it should be the same reference
    expect(result.children[1]).toBe(filterB);
    // filterA was changed (different child), so it's a new object
    expect(result.children[0]).not.toBe(filterA);
  });
});

describe('walkTree', () => {
  it('visits all nodes in pre-order', () => {
    const tree = makeSimpleTree();
    const visited: string[] = [];

    walkTree(tree, (node) => {
      visited.push(node.id);
    });

    // Pre-order: pipeline, sink, filter, source
    expect(visited).toEqual(['pipeline', 'sink', 'filter', 'source']);
  });

  it('skips children when callback returns false', () => {
    const tree = makeSimpleTree();
    const visited: string[] = [];

    walkTree(tree, (node) => {
      visited.push(node.id);
      if (node.id === 'sink') return false; // skip sink's children
    });

    expect(visited).toEqual(['pipeline', 'sink']);
  });
});

describe('findNodes', () => {
  it('finds nodes matching a predicate', () => {
    const tree = makeSimpleTree();

    const sources = findNodes(tree, (n) => n.kind === 'Source');
    expect(sources).toHaveLength(1);
    expect(sources[0].id).toBe('source');
  });

  it('finds multiple matches', () => {
    const source1 = makeNode('s1', 'Source');
    const source2 = makeNode('s2', 'Source');
    const pipeline = makeNode('pipeline', 'Pipeline', [source1, source2]);

    const sources = findNodes(pipeline, (n) => n.kind === 'Source');
    expect(sources).toHaveLength(2);
  });

  it('returns empty array when nothing matches', () => {
    const tree = makeSimpleTree();
    const matches = findNodes(tree, (n) => n.component === 'NonExistent');
    expect(matches).toEqual([]);
  });
});

describe('wrapNode', () => {
  it('inserts a wrapper above the target node', () => {
    const tree = makeSimpleTree();

    const wrapper: Omit<ConstructNode, 'children'> = {
      id: 'wrapper',
      kind: 'Transform',
      component: 'Wrapper',
      props: { added: true },
    };

    const result = wrapNode(tree, 'source', wrapper);

    // The filter's child should now be the wrapper
    const filter = result.children[0].children[0];
    expect(filter.children[0].id).toBe('wrapper');
    expect(filter.children[0].component).toBe('Wrapper');

    // The wrapper's child should be the original source
    const wrappedSource = filter.children[0].children[0];
    expect(wrappedSource.id).toBe('source');
    expect(wrappedSource.kind).toBe('Source');
  });

  it('returns unchanged tree when target ID not found', () => {
    const tree = makeSimpleTree();
    const wrapper: Omit<ConstructNode, 'children'> = {
      id: 'w', kind: 'Transform', component: 'W', props: {},
    };

    const result = wrapNode(tree, 'nonexistent', wrapper);
    expect(result).toBe(tree); // Same reference — nothing changed
  });
});

describe('replaceChild', () => {
  it('replaces a child by its ID', () => {
    const tree = makeSimpleTree();
    const newSource = makeNode('new-source', 'Source');

    const result = replaceChild(tree, 'source', newSource);

    const filter = result.children[0].children[0];
    expect(filter.children[0].id).toBe('new-source');
  });

  it('returns unchanged tree when child ID not found', () => {
    const tree = makeSimpleTree();
    const replacement = makeNode('r', 'Source');

    const result = replaceChild(tree, 'nonexistent', replacement);
    expect(result).toBe(tree);
  });

  it('only replaces the first matching child', () => {
    const childA = makeNode('target', 'Source');
    const childB = makeNode('other', 'Source');
    const parent = makeNode('parent', 'Pipeline', [childA, childB]);

    const replacement = makeNode('replaced', 'Source');
    const result = replaceChild(parent, 'target', replacement);

    expect(result.children[0].id).toBe('replaced');
    expect(result.children[1].id).toBe('other');
  });
});
