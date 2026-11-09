import { test } from '../lib/fixtures';
import { emptyAnswerPollsAndResolves, storedAnswerSurvivesRecovery, withheldAndRefusedAnswersClearData } from '../lib/storedRefreshBrowser';

test('connectors: stale authorized data counts as loaded and survives finite background recovery', async ({ page, signedIn }) => {
  void signedIn;
  await storedAnswerSurvivesRecovery(page);
});
test('connectors: empty answers short-poll until a computed-empty answer is loaded', async ({ page, signedIn }) => {
  void signedIn;
  await emptyAnswerPollsAndResolves(page);
});
test('connectors: changed authority withholds retained data and refusal stops polling', async ({ page, signedIn }) => {
  void signedIn;
  await withheldAndRefusedAnswersClearData(page);
});
