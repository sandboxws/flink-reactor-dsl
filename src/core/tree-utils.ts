import type { ConstructNode, NodeKind } from './types.js';

// ── rekindTree ──────────────────────────────────────────────────────

/**
 * Re-resolve node kinds in a construct tree using an updated kind map.
 * This is needed when plugins register new component kinds after
 * the tree was built via createElement().
 *
 * Only updates nodes whose component matches a key in the provided map.
 * Returns the same tree reference if no changes are needed.
 */
export function rekindTree(
  root: ConstructNode,
  kindMap: ReadonlyMap<string, NodeKind>,
): ConstructNode {
  return mapTree(root, (node) => {
    const newKind = kindMap.get(node.component);
    if (newKind !== undefined && newKind !== node.kind) {
      return { ...node, kind: newKind };
    }
    return node;
  });
}

// ── mapTree ─────────────────────────────────────────────────────────

/**
 * Post-order traversal that builds a new tree by applying a visitor
 * to each node. Children are visited first, then the visitor receives
 * the node with its (potentially replaced) children.
 *
 * If the visitor returns the same node reference, no copy is made
 * for that subtree (structural sharing).
 *
 * @param root - The root of the construct tree
 * @param visitor - Function that receives a node (with updated children) and returns a replacement
 * @returns A new tree root with visitor-applied transformations
 */
export function mapTree(
  root: ConstructNode,
  visitor: (node: ConstructNode) => ConstructNode,
): ConstructNode {
  // Post-order: visit children first
  const mappedChildren = root.children.map((child) => mapTree(child, visitor));

  // Check if children changed (structural sharing)
  const childrenChanged = mappedChildren.some((c, i) => c !== root.children[i]);
  const nodeWithChildren: ConstructNode = childrenChanged
    ? { ...root, children: mappedChildren }
    : root;

  return visitor(nodeWithChildren);
}

// ── walkTree ────────────────────────────────────────────────────────

/**
 * Pre-order read-only traversal. Calls the callback for each node
 * in the tree, parent before children.
 *
 * @param root - The root of the construct tree
 * @param callback - Called for each node; return false to skip children
 */
export function walkTree(
  root: ConstructNode,
  callback: (node: ConstructNode) => void | false,
): void {
  const result = callback(root);
  if (result === false) return;

  for (const child of root.children) {
    walkTree(child, callback);
  }
}

// ── findNodes ───────────────────────────────────────────────────────

/**
 * Collect all nodes in the tree matching a predicate.
 * Uses pre-order traversal.
 *
 * @param root - The root of the construct tree
 * @param predicate - Test function for each node
 * @returns Array of matching nodes
 */
export function findNodes(
  root: ConstructNode,
  predicate: (node: ConstructNode) => boolean,
): ConstructNode[] {
  const results: ConstructNode[] = [];
  walkTree(root, (node) => {
    if (predicate(node)) {
      results.push(node);
    }
  });
  return results;
}

// ── wrapNode ────────────────────────────────────────────────────────

/**
 * Insert a wrapper node as the parent of a target node.
 * The wrapper's children will be set to `[targetNode]`.
 *
 * Uses mapTree to find the target by ID and replace it in-place.
 *
 * @param root - The root of the construct tree
 * @param targetId - ID of the node to wrap
 * @param wrapper - A node whose children will be replaced with [target]
 * @returns New tree with the wrapper inserted
 */
export function wrapNode(
  root: ConstructNode,
  targetId: string,
  wrapper: Omit<ConstructNode, 'children'>,
): ConstructNode {
  return mapTree(root, (node) => {
    if (node.id === targetId) {
      return { ...wrapper, children: [node] };
    }
    return node;
  });
}

// ── replaceChild ────────────────────────────────────────────────────

/**
 * Replace a direct child of a parent node by the child's ID.
 *
 * Searches the tree for any node that has a child with the given ID,
 * and replaces that child with the replacement node.
 *
 * @param root - The root of the construct tree
 * @param childId - ID of the child node to replace
 * @param replacement - The replacement node
 * @returns New tree with the child replaced
 */
export function replaceChild(
  root: ConstructNode,
  childId: string,
  replacement: ConstructNode,
): ConstructNode {
  return mapTree(root, (node) => {
    const idx = node.children.findIndex((c) => c.id === childId);
    if (idx === -1) return node;

    const newChildren = [...node.children];
    newChildren[idx] = replacement;
    return { ...node, children: newChildren };
  });
}
