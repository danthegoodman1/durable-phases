import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { SqliteDurabilityProvider } from "../src/durable.js"
import { describeDurabilityProviderConformance } from "../src/testing/conformance.js"

describeDurabilityProviderConformance({
  name: "SqliteDurabilityProvider",
  async createStore() {
    const dir = await mkdtemp(join(tmpdir(), "durable-poc-conformance-"))
    const path = join(dir, "store.sqlite")
    return {
      createProvider() {
        const provider = new SqliteDurabilityProvider(path)
        return {
          provider,
          close: () => provider.close(),
        }
      },
      async cleanup() {
        await rm(dir, { force: true, maxRetries: 3, recursive: true, retryDelay: 10 })
      },
    }
  },
})
