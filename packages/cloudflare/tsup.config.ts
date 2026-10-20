import { defineConfig } from 'tsup';

export default defineConfig({
  entry: ['src/index.ts', 'src/client/index.ts', 'src/workflow/index.ts', 'src/workers/index.ts'],
  format: ['esm'],
  dts: true,
  clean: true,
  sourcemap: true,
  target: 'es2022',
  platform: 'neutral',
  // Runtime-provided modules: resolved by workerd inside the consumer's Worker.
  external: ['cloudflare:workers', 'cloudflare:workflows'],
  splitting: false,
});
