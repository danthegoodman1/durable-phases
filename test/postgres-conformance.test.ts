import { randomUUID } from "node:crypto"
import { describe } from "vitest"
import { describeDurabilityProviderConformance } from "../src/testing/conformance.js"
import { PostgresDurabilityProvider } from "../src/durable.js"

const connectionString = process.env.DURABLE_POSTGRES_URL

if (connectionString) {
  describeDurabilityProviderConformance({
    name: "PostgresDurabilityProvider",
    createStore() {
      const schema = `durable_test_${randomUUID().replaceAll("-", "_")}`
      return {
        async createProvider() {
          const provider = await PostgresDurabilityProvider.create({
            connectionString,
            schema,
            poolSize: 8,
          })
          return {
            provider,
            close: () => provider.close(),
          }
        },
        async cleanup() {
          const provider = await PostgresDurabilityProvider.create({
            connectionString,
            schema,
            poolSize: 1,
          })
          try {
            await provider.dropSchema()
          } finally {
            await provider.close()
          }
        },
      }
    },
  })
} else {
  describe.skip("PostgresDurabilityProvider durability provider conformance", () => {
    // Requires DURABLE_POSTGRES_URL and is run by npm run test:postgres.
  })
}
