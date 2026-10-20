import { fileURLToPath } from 'node:url';

import { defineConfig } from 'vitest/config';

export default defineConfig({
  resolve: {
    alias: {
      // The runtime modules only exist inside workerd; tests run on Node against stubs.
      'cloudflare:workflows': fileURLToPath(new URL('test/stubs/cloudflare-workflows.ts', import.meta.url)),
      'cloudflare:workers': fileURLToPath(new URL('test/stubs/cloudflare-workers.ts', import.meta.url)),
    },
  },
  test: { include: ['src/**/*.test.ts', 'test/**/*.test.ts'] },
});
