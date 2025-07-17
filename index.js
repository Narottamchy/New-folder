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

// Load local state
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



const app = express();


app.get('/start-campaign', async (req, res) => {
  try {
    const batchSize = parseInt(req.query.batchSize) || 100;
    const lastReceiver = readLocal('state/last_receiver.txt');
    const lastSender = readLocal('state/last_sender.txt');
    const totalSent = parseInt(readLocal('state/total_sent.txt') || '0');

    // Use S3 utility to get unsubscribed and email list
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

      // Use SES utility to send email
      await sendTemplateEmail({
        to: r.Email,
        from: sender,
        templateData
      });

      logger.info(`Email sent to ${r.Email} from ${sender}`);

      writeLocal('state/last_receiver.txt', r.Email);
      writeLocal('state/last_sender.txt', sender);
      writeLocal('state/total_sent.txt', (totalSent + sentCount + 1).toString());

      sender = nextSender(sender);
      sentCount++;
    }

    res.send(`✅ Sent ${sentCount} emails this run`);
  } catch (err) {
    logger.error(`❌ Error: ${err.message}`);
    res.status(500).send('Internal Server Error');
  }
});

app.listen(PORT, () => {
  logger.info(`Server started on port ${PORT}`);
});
