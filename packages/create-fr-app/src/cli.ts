import { Command } from "commander";
import { init } from "./commands/init.js";
import { add } from "./commands/add.js";
import { check } from "./commands/check.js";
import { update } from "./commands/update.js";

const program = new Command();

program
  .name("create-fr-app")
  .description("CLI to create and manage apps with the FlinkReactor design system")
  .version("0.1.0");

program
  .command("init")
  .argument("[project-name]", "Name of the project to create")
  .description("Create a new app with the FlinkReactor design system")
  .option("-t, --template <template>", "Template to use", "nextjs")
  .option("--no-git", "Skip git initialization")
  .option("--no-install", "Skip dependency installation")
  .action(init);

program
  .command("add")
  .argument("<components...>", "Components to add")
  .description("Add components to an existing project")
  .option("-y, --yes", "Skip confirmation")
  .action(add);

program
  .command("check")
  .description("Check for available updates")
  .action(check);

program
  .command("update")
  .description("Update components to latest versions")
  .option("-y, --yes", "Skip confirmation and accept all updates")
  .option("--force", "Overwrite local changes")
  .action(update);

// If called with no command, run init
if (process.argv.length === 2) {
  program.parse(["node", "create-fr-app", "init"]);
} else {
  program.parse();
}
