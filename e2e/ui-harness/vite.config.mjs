import { defineConfig } from '../../web/node_modules/vite/dist/node/index.js';
import { fileURLToPath } from 'node:url';
const path = p => fileURLToPath(new URL(p, import.meta.url));
export default defineConfig({ root: path('./'), esbuild: {jsx: 'automatic'}, define: {'process.env': '{}'}, resolve: {alias: [
{find: '@clerk/nextjs', replacement: path('./clerk.ts')},
{find: /^(?:@\/lib\/providers|\.\/providers)$/, replacement: path('./providers.ts')},
{find: 'next/navigation', replacement: path('./navigation.ts')},
{find: 'next/link', replacement: path('./link.tsx')},
{find: '@', replacement: path('../../web')},
{find: 'react', replacement: path('../../web/node_modules/react')},
{find: 'react-dom', replacement: path('../../web/node_modules/react-dom')},
{find: '@tanstack/react-query', replacement: path('../../web/node_modules/@tanstack/react-query')}
]}, server: {host:'127.0.0.1', port:23600, strictPort:true, fs:{allow:[path('../../')]} }});
