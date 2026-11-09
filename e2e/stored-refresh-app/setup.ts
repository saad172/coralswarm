import { clerkSetup } from '@clerk/testing/playwright';

export default async function setup() {
  await clerkSetup();
}
