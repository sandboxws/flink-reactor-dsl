import { mkdirSync, writeFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import type { ResolvedDashboardJson } from '../core/config-resolver.js';

/**
 * Write the resolved dashboard config JSON to .flink-reactor/.
 * Creates the directory if it doesn't exist.
 *
 * @returns The absolute path to the written file.
 */
export function writeResolvedDashboardConfig(
  projectDir: string,
  json: ResolvedDashboardJson,
  outputPath?: string,
): string {
  const filePath = outputPath ?? join(projectDir, '.flink-reactor', 'resolved-dashboard.json');
  mkdirSync(dirname(filePath), { recursive: true });
  writeFileSync(filePath, JSON.stringify(json, null, 2) + '\n', 'utf-8');
  return filePath;
}
