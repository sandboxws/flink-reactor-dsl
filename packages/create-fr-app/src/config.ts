/**
 * Component registry - maps component names to their paths and dependencies
 */
export interface ComponentInfo {
  /** Path relative to packages/ui/src */
  path: string
  /** Other components this depends on */
  deps: string[]
  /** npm peer dependencies required */
  peerDeps?: string[]
  /** Description for CLI help */
  description?: string
}

export const COMPONENT_REGISTRY: Record<string, ComponentInfo> = {
  // ── Utilities ────────────────────────────────────────
  cn: {
    path: "lib/cn.ts",
    deps: [],
    description: "Tailwind class merge utility",
  },
  constants: {
    path: "lib/constants.ts",
    deps: [],
    description: "Design system constants (colors, limits, formats)",
  },

  // ── UI Components ────────────────────────────────────
  button: {
    path: "components/ui/button.tsx",
    deps: ["cn"],
    description: "Button with variants and sizes",
  },
  badge: {
    path: "components/ui/badge.tsx",
    deps: ["cn"],
    description: "Badge with variants",
  },
  card: {
    path: "components/ui/card.tsx",
    deps: ["cn"],
    description: "Card container with header, content, footer",
  },
  input: {
    path: "components/ui/input.tsx",
    deps: ["cn"],
    description: "Text input field",
  },
  label: {
    path: "components/ui/label.tsx",
    deps: ["cn"],
    peerDeps: ["@radix-ui/react-label"],
    description: "Form label",
  },
  textarea: {
    path: "components/ui/textarea.tsx",
    deps: ["cn"],
    description: "Multi-line text input",
  },
  table: {
    path: "components/ui/table.tsx",
    deps: ["cn"],
    description: "Table with header, body, rows, cells",
  },
  tabs: {
    path: "components/ui/tabs.tsx",
    deps: ["cn"],
    peerDeps: ["@radix-ui/react-tabs"],
    description: "Tabbed interface",
  },
  dialog: {
    path: "components/ui/dialog.tsx",
    deps: ["cn"],
    peerDeps: ["@radix-ui/react-dialog"],
    description: "Modal dialog",
  },
  tooltip: {
    path: "components/ui/tooltip.tsx",
    deps: ["cn"],
    peerDeps: ["@radix-ui/react-tooltip"],
    description: "Tooltip on hover",
  },
  collapsible: {
    path: "components/ui/collapsible.tsx",
    deps: [],
    peerDeps: ["@radix-ui/react-collapsible"],
    description: "Collapsible content section",
  },
  popover: {
    path: "components/ui/popover.tsx",
    deps: ["cn"],
    peerDeps: ["@radix-ui/react-popover"],
    description: "Popover overlay",
  },
  "hover-card": {
    path: "components/ui/hover-card.tsx",
    deps: ["cn"],
    peerDeps: ["@radix-ui/react-hover-card"],
    description: "Card shown on hover",
  },
  select: {
    path: "components/ui/select.tsx",
    deps: ["cn"],
    peerDeps: ["@radix-ui/react-select"],
    description: "Dropdown select menu",
  },
  progress: {
    path: "components/ui/progress.tsx",
    deps: ["cn"],
    peerDeps: ["@radix-ui/react-progress"],
    description: "Progress bar",
  },
  separator: {
    path: "components/ui/separator.tsx",
    deps: ["cn"],
    peerDeps: ["@radix-ui/react-separator"],
    description: "Visual separator line",
  },
  resizable: {
    path: "components/ui/resizable.tsx",
    deps: ["cn"],
    peerDeps: ["react-resizable-panels"],
    description: "Resizable panel layout",
  },

  // ── Layout Components ────────────────────────────────
  shell: {
    path: "layout/shell.tsx",
    deps: ["cn"],
    description: "Root app shell with sidebar/header",
  },
  sidebar: {
    path: "layout/sidebar.tsx",
    deps: ["cn"],
    description: "Collapsible navigation sidebar",
  },
  header: {
    path: "layout/header.tsx",
    deps: ["cn"],
    description: "Header bar with breadcrumbs",
  },
  "command-palette": {
    path: "layout/command-palette.tsx",
    deps: [],
    peerDeps: ["cmdk"],
    description: "Cmd+K command palette",
  },

  // ── Shared Components ────────────────────────────────
  "metric-card": {
    path: "shared/metric-card.tsx",
    deps: ["cn"],
    description: "Glass-effect metric display card",
  },
  "empty-state": {
    path: "shared/empty-state.tsx",
    deps: [],
    description: "Empty content placeholder",
  },
  "text-viewer": {
    path: "shared/text-viewer.tsx",
    deps: ["cn", "tooltip"],
    description: "Monospace text viewer with line numbers",
  },
  "search-input": {
    path: "shared/search-input.tsx",
    deps: ["cn"],
    description: "Search input with regex toggle",
  },
  "severity-badge": {
    path: "shared/severity-badge.tsx",
    deps: [],
    description: "Log severity badge (TRACE, DEBUG, INFO, WARN, ERROR)",
  },
  "source-badge": {
    path: "shared/source-badge.tsx",
    deps: [],
    description: "Log source badge (JM, TM, CLI)",
  },
  "time-range": {
    path: "shared/time-range.tsx",
    deps: ["cn"],
    description: "Time range preset selector",
  },
}

/**
 * Style files to copy
 */
export const STYLE_FILES = [
  "styles/tokens.css",
  "styles/components.css",
  "styles/index.css",
]

/**
 * Collect all dependencies for a set of components
 */
export function collectDependencies(components: string[]): Set<string> {
  const deps = new Set<string>()
  const queue = [...components]

  while (queue.length > 0) {
    const comp = queue.shift()!
    if (deps.has(comp)) continue

    const info = COMPONENT_REGISTRY[comp]
    if (!info) continue

    deps.add(comp)
    queue.push(...info.deps)
  }

  return deps
}

/**
 * Get npm packages required for a set of components
 */
export function collectPeerDeps(components: string[]): string[] {
  const peerDeps = new Set<string>()

  // Always include tailwind-merge for cn utility
  peerDeps.add("tailwind-merge@^3.0.0")

  for (const comp of components) {
    const info = COMPONENT_REGISTRY[comp]
    if (info?.peerDeps) {
      for (const dep of info.peerDeps) {
        peerDeps.add(dep)
      }
    }
  }

  return [...peerDeps]
}
