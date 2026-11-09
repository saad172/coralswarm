import { randomBytes } from 'node:crypto';
import { existsSync, writeFileSync } from 'node:fs';
const pk = process.env.NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY ?? '';
if (!pk.startsWith('pk_test_') || !process.env.CLERK_SECRET_KEY?.startsWith('sk_test_')) throw Error('Use your own Clerk DEVELOPMENT keys');
const host = Buffer.from(pk.slice(8), 'base64').toString().replace(/\$$/, '');
if (!/^[a-zA-Z0-9.-]+$/.test(host)) throw Error('Invalid Clerk publishable key');
if (!existsSync('/local/secrets')) writeFileSync('/local/secrets', ['MASTER_KEK_HEX','ACCOUNT_MASTER_KEK','INTERNAL_TOKEN','OAUTH_STATE_SECRET'].map(k => `${k}=${randomBytes(32).toString('hex')}`).join('\n'), {mode:0o600});
const { readFileSync } = await import('node:fs');
writeFileSync('/local/config', readFileSync('/local/secrets','utf8') + `\nCLERK_ISSUER=https://${host}\n`, {mode:0o600});
