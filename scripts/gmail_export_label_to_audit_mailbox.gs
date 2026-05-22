/**
 * Export Gmail messages from a label into an audit mailbox as .eml attachments.
 *
 * Why this exists:
 * - Gmail filters only auto-forward new messages, not old labeled mail.
 * - "Forward as attachment" in the UI works, but is manual and messy in batches.
 * - This script preserves each original message as an .eml attachment and
 *   labels processed messages so reruns are idempotent.
 *
 * Setup:
 * 1. Open https://script.google.com while signed into the source Gmail account.
 * 2. Create a new Apps Script project.
 * 3. Paste this file in.
 * 4. Edit CONFIG below.
 * 5. Run exportPromoLabelBatch().
 *
 * Google references:
 * - Gmail filters forward only new mail:
 *   https://support.google.com/mail/answer/6579
 * - Gmail can send emails as .eml attachments:
 *   https://support.google.com/mail/answer/9261412
 * - Apps Script GmailMessage.getRawContent():
 *   https://developers.google.com/apps-script/reference/gmail/gmail-message
 */

const CONFIG = {
  sourceLabel: 'Promo Audit',
  destinationEmail: 'mission.forms@gmail.com',
  processedLabel: 'Promo Audit/Exported',
  errorLabel: 'Promo Audit/ExportError',
  batchSize: 20,
  dryRun: false,
  // Optional extra search narrowing. Example: 'from:NICOLEB946@missionfoods.com'
  additionalQuery: '',
};

function exportPromoLabelBatch() {
  const sourceLabel = GmailApp.getUserLabelByName(CONFIG.sourceLabel);
  if (!sourceLabel) {
    throw new Error(`Missing source label: ${CONFIG.sourceLabel}`);
  }

  const processedLabel = getOrCreateLabel_(CONFIG.processedLabel);
  const errorLabel = getOrCreateLabel_(CONFIG.errorLabel);

  const queryParts = [`label:"${CONFIG.sourceLabel}"`, `-label:"${CONFIG.processedLabel}"`];
  if (CONFIG.additionalQuery) queryParts.push(CONFIG.additionalQuery);
  const query = queryParts.join(' ');

  const threads = GmailApp.search(query, 0, CONFIG.batchSize);
  Logger.log(`Found %s threads for batch`, threads.length);

  let exportedCount = 0;
  for (const thread of threads) {
    const messages = thread.getMessages();
    const blobs = [];
    const ids = [];

    try {
      for (const message of messages) {
        if (messageHasLabel_(message, processedLabel)) {
          continue;
        }

        const subject = safeFilenamePart_(message.getSubject()) || 'message';
        const datePart = Utilities.formatDate(message.getDate(), Session.getScriptTimeZone(), 'yyyy-MM-dd');
        const blob = Utilities.newBlob(message.getRawContent(), 'message/rfc822', `${datePart} - ${subject}.eml`);
        blobs.push(blob);
        ids.push(message.getId());
      }

      if (!blobs.length) {
        continue;
      }

      if (CONFIG.dryRun) {
        Logger.log('DRY RUN: would export %s messages from thread %s', blobs.length, thread.getFirstMessageSubject());
      } else {
        GmailApp.sendEmail(
          CONFIG.destinationEmail,
          `Promo export: ${thread.getFirstMessageSubject()}`,
          buildBody_(thread, ids),
          { attachments: blobs }
        );
      }

      thread.addLabel(processedLabel);
      thread.removeLabel(errorLabel);
      exportedCount += blobs.length;
    } catch (err) {
      thread.addLabel(errorLabel);
      Logger.log('Export failed for thread "%s": %s', thread.getFirstMessageSubject(), err);
    }
  }

  Logger.log('Exported %s messages in this batch', exportedCount);
}

function exportUntilDone(maxBatches) {
  const batches = maxBatches || 25;
  for (let i = 0; i < batches; i++) {
    const before = countPending_();
    if (before === 0) {
      Logger.log('No pending messages left');
      return;
    }
    exportPromoLabelBatch();
    const after = countPending_();
    Logger.log('Pending before=%s after=%s', before, after);
    if (after >= before) {
      Logger.log('No progress in latest batch; stopping');
      return;
    }
  }
}

function countPending_() {
  const queryParts = [`label:"${CONFIG.sourceLabel}"`, `-label:"${CONFIG.processedLabel}"`];
  if (CONFIG.additionalQuery) queryParts.push(CONFIG.additionalQuery);
  return GmailApp.search(queryParts.join(' '), 0, 500).length;
}

function getOrCreateLabel_(name) {
  return GmailApp.getUserLabelByName(name) || GmailApp.createLabel(name);
}

function messageHasLabel_(message, label) {
  const labels = message.getThread().getLabels().map((item) => item.getName());
  return labels.includes(label.getName());
}

function safeFilenamePart_(value) {
  return String(value || '')
    .replace(/[\\/:*?"<>|]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 120);
}

function buildBody_(thread, ids) {
  return [
    'Automated promo export from Gmail label.',
    '',
    `Source thread subject: ${thread.getFirstMessageSubject()}`,
    `Source message count in this export: ${ids.length}`,
    `Source Gmail message ids: ${ids.join(', ')}`,
  ].join('\n');
}
