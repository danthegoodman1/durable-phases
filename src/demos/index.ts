import { pathToFileURL } from "node:url"
import { runChildWorkflowDemo } from "./child-workflow.js"
import { runCustomRunnerDemo } from "./custom-runner.js"
import { runImmediateAndSignalDemo } from "./immediate-and-signal.js"
import { runMigrationDemo } from "./migration.js"
import { runStayLoopDemo } from "./stay-loop.js"
import { runTimerStayRestartDemo } from "./timer-stay-restart.js"

export async function runAllDemos(): Promise<void> {
  await runImmediateAndSignalDemo()
  await runTimerStayRestartDemo()
  await runStayLoopDemo()
  await runCustomRunnerDemo()
  await runChildWorkflowDemo()
  await runMigrationDemo()
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  await runAllDemos()
}
