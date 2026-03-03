/**
 * Publish the root `flink-reactor` package to npm.
 *
 * `changeset publish` only handles workspace member packages (packages/*, apps/*).
 * The root package lives at the workspace root, so it must be published separately.
 * This script checks whether the current version already exists on npm and only
 * publishes when it doesn't — making it safe to run on every release.
 */

import { execSync } from "node:child_process"
import { readFileSync } from "node:fs"

const { name, version } = JSON.parse(readFileSync("./package.json", "utf8"))

try {
  execSync(`npm view ${name}@${version} version`, { stdio: "pipe" })
  console.log(`${name}@${version} already on npm, skipping`)
} catch {
  console.log(`Publishing ${name}@${version}...`)
  execSync("npm publish --access public", { stdio: "inherit" })
}
