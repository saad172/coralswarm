import { test } from '@playwright/test';
import { emptyAnswerPollsAndResolves, storedAnswerSurvivesRecovery, withheldAndRefusedAnswersClearData } from '../lib/storedRefreshBrowser';

// Same assertions as the full-app scenarios. The lean harness supplies auth
// and Next shims; it makes no backend or full-app routing claims.
test('stored answers stay useful with finite polling', async ({ page }) => { await storedAnswerSurvivesRecovery(page); });
test('empty answers poll until computed', async ({ page }) => { await emptyAnswerPollsAndResolves(page); });
test('authority changes clear retained data', async ({ page }) => { await withheldAndRefusedAnswersClearData(page); });
