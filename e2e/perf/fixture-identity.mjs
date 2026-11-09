import { identityFromTestEmail, identityFromTestUser, testUserMetadata } from '../stack/clerk-identity.mjs';

// Backend invitation creates a real participant outside createUser's hook.
// Check both the intended address and ALL actual user addresses/metadata
// before the caller's sole authorized metadata write. No SDK import here.
export async function adoptInvitedParticipant({ identity, email, user, updateMetadata }) {
  if (identity.namespace !== 'nightly' || !identity.lease || identity.lease.expiresAt <= Date.now()) throw new Error('live nightly owner lease required');
  const sameOwner = value => value?.namespace === identity.namespace && value.owner === identity.owner;
  if (!sameOwner(identityFromTestEmail(email)) || !sameOwner(identityFromTestUser(user)) ||
    !user.emailAddresses?.some(address => address.emailAddress === email)) throw new Error('invited participant address/namespace/owner mismatch');
  const metadata = testUserMetadata(identity);
  await updateMetadata(user.id, { privateMetadata: metadata });
  return { namespace_matches: true, owner_matches: true, email_matches: true, lease_attached: true };
}
