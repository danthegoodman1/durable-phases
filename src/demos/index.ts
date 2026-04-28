import { pathToFileURL } from "node:url"
import { runChildWorkflowDemo } from "./child-workflow.js"
import { runCheckpointLoopDemo } from "./checkpoint-loop.js"
import { runImmediateAndSignalDemo } from "./immediate-and-signal.js"
import { runTimerStayRestartDemo } from "./timer-stay-restart.js"

export async function runAllDemos(): Promise<void> {
  await runImmediateAndSignalDemo()
  await runTimerStayRestartDemo()
  await runCheckpointLoopDemo()
  await runChildWorkflowDemo()
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  await runAllDemos()
}
