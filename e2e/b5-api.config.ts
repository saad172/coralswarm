import config from './playwright.config';
// API-only acceptance uses real Clerk session JWTs; no browser setup required.
export default { ...config, globalSetup: undefined };
