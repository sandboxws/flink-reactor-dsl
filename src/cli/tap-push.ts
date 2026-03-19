import pc from "picocolors"
import type { TapManifest } from "@/core/types.js"

/**
 * Push a tap manifest to the reactor-console backend.
 *
 * Posts the manifest JSON to the console's REST API.
 * Never throws — logs a warning on failure and returns false.
 */
export async function pushTapManifest(
  manifest: TapManifest,
  consoleUrl: string,
): Promise<boolean> {
  const url = `${consoleUrl.replace(/\/$/, "")}/api/tap-manifests`

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(manifest),
      signal: AbortSignal.timeout(5000),
    })

    if (!response.ok) {
      const body = await response.text().catch(() => "")
      console.log(
        pc.yellow(
          `  Warning: failed to push tap manifest (${response.status}): ${body}`,
        ),
      )
      return false
    }

    console.log(
      pc.dim(`  Tap manifest pushed to console (${manifest.pipelineName})`),
    )
    return true
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err)
    console.log(pc.yellow(`  Warning: could not push tap manifest: ${message}`))
    return false
  }
}
