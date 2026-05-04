---
"@flink-reactor/dsl": minor
---

Add opt-in Prometheus + Grafana stack to the local cluster (`services.grafana: {}`). Grafana is themed with Gruvppuccin to match FlinkReactor Console and ships two auto-provisioned dashboards: **Flink Cluster Overview** and **pg-fluss-paimon Pipeline**. Both Prometheus (`localhost:9090`) and Grafana (`localhost:3000`) are surfaced via `cluster open <target>`. The Flink JM/TM `FLINK_PROPERTIES` now wire the Prometheus reporter on port 9249, and `Dockerfile.flink` activates the bundled reporter JAR on the runtime classpath.

`fr new` learned a Grafana opt-in (interactive prompt + `--grafana` / `--no-grafana` flags), gated to Flink 2.x because the reporter wiring and dashboard PromQL are 2.x-only. Picking yes injects `services.grafana: {}` and the matching `metricsPlugin` registration into the rendered `flink-reactor.config.ts` — both pieces ship together so Prometheus actually sees Flink metrics. Default is off.
