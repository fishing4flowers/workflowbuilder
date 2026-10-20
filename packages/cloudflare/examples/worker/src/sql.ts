// One connection per request/invocation, as Hyperdrive expects. postgres.js runs on
// Workers with `nodejs_compat`; @neondatabase/serverless is the other common choice.
import postgres from 'postgres';

import type { SqlTag } from '@workflowbuilder/cloudflare/workers';

export function openSql(env: { DB: Hyperdrive }): SqlTag {
  return postgres(env.DB.connectionString, { max: 1, prepare: false }) as unknown as SqlTag;
}
