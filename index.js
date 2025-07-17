require('dotenv').config();
const fs = require('fs');
const express = require('express');
const csv = require('csv-parser');
const path = require('path');
const { logger } = require('./utils/logger');
const { getObject } = require('./utils/s3');
const { sendTemplateEmail } = require('./utils/ses');

const {
  SENDERS,
  EMAIL_LIST_KEY,
  UNSUBSCRIBED_KEY,
  UNSUBSCRIBE_URL,
  SES_TEMPLATE,
  NOTIFY_EMAIL,
  PORT
} = process.env;

const senders = SENDERS.split(',');
const app = express();

let isRunning = false; // Flag to prevent multiple runs

// Helper functions
const readLocal = (file) =>
  fs.existsSync(file) ? fs.readFileSync(file, 'utf8') : '';
const writeLocal = (file, data) =>
  fs.writeFileSync(file, data, 'utf8');
const readCSVFromString = (str) => {
  return new Promise((resolve) => {
    const results = [];
    require('stream').Readable.from(str)
      .pipe(csv())
      .on('data', (data) => results.push(data))
      .on('end', () => resolve(results));
  });
};
const nextSender = (last) => {
  const idx = senders.indexOf(last);
  return idx === -1 ? senders[0] : senders[(idx + 1) % senders.length];
};

const campaignStatus = {
  running: false,
  totalSent: 0,
  lastReceiver: '',
  lastSender: '',
  startTime: null,
};

// Main background function
async function runCampaign(batchSize) {
  try {
    campaignStatus.running = true;
    campaignStatus.startTime = new Date().toISOString();

    const lastReceiver = readLocal('state/last_receiver.txt');
    const lastSender = readLocal('state/last_sender.txt');
    const totalSent = parseInt(readLocal('state/total_sent.txt') || '0');

    const unsubscribed = new Set(
      (await getObject(UNSUBSCRIBED_KEY)).split('\n').map(e => e.trim().toLowerCase())
    );
    const emailCSV = await getObject(EMAIL_LIST_KEY);
    const recipients = await readCSVFromString(emailCSV);

    let startIdx = lastReceiver
      ? recipients.findIndex(r => r.Email === lastReceiver) + 1
      : 0;

    let sender = nextSender(lastSender);
    let sentCount = 0;

    for (let i = startIdx; i < recipients.length && sentCount < batchSize; i++) {
      const r = recipients[i];
      if (!r.Email || unsubscribed.has(r.Email.toLowerCase())) continue;

      const templateData = {
        INSTAHANDLE: `@${r.Username}`,
        URL: `${UNSUBSCRIBE_URL}?email=${encodeURIComponent(r.Email)}`
      };

      await sendTemplateEmail({
        to: r.Email,
        from: sender,
        templateData
      });

      logger.info(`Email sent to ${r.Email} from ${sender}`);

      writeLocal('state/last_receiver.txt', r.Email);
      writeLocal('state/last_sender.txt', sender);
      writeLocal('state/total_sent.txt', (totalSent + sentCount + 1).toString());

      campaignStatus.lastReceiver = r.Email;
      campaignStatus.lastSender = sender;
      campaignStatus.totalSent = totalSent + sentCount + 1;

      sender = nextSender(sender);
      sentCount++;
    }

    logger.info(`✅ Completed batch: ${sentCount} emails sent`);
  } catch (err) {
    logger.error(`❌ Campaign error: ${err.message}`);
  } finally {
    campaignStatus.running = false;
  }
}

// Start campaign API
app.get('/start-campaign', async (req, res) => {
  if (campaignStatus.running) {
    return res.status(400).send('⚠️ Campaign is already running');
  }

  const batchSize = parseInt(req.query.batchSize) || 100;

  logger.info('🔁 Campaign triggered');
  setImmediate(() => runCampaign(batchSize)); // Run in background

  res.send(`📤 Campaign started in background (batchSize: ${batchSize})`);
});

// Status check API
app.get('/campaign-status', (req, res) => {
  res.json(campaignStatus);
});

app.listen(PORT, () => {
  logger.info(`🚀 Server running on port ${PORT}`);
});
