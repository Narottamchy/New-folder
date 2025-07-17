require("dotenv").config();
const express = require("express");
const fs = require("fs");
const path = require("path");
const AWS = require("aws-sdk");
const csv = require("csv-parser");
const app = express();
const logger = require("./logger");

const ses = new AWS.SES({ region: process.env.AWS_REGION });
const s3 = new AWS.S3();

const SENDERS = process.env.SENDERS.split(",");
const SUBJECT = process.env.SUBJECT;
const HTML_TEMPLATE_PATH = process.env.HTML_TEMPLATE_PATH;
const MAX_TOTAL_EMAILS = parseInt(process.env.MAX_TOTAL_EMAILS, 10);
const CONFIG_SET = process.env.CONFIG_SET;
const NOTIFY_EMAIL = process.env.NOTIFY_EMAIL;
const BUCKET_NAME = process.env.BUCKET_NAME;
const EMAIL_CSV_KEY = process.env.EMAIL_CSV_KEY;
const UNSUBSCRIBED_KEY = process.env.UNSUBSCRIBED_KEY;

const STATE_PATH = path.resolve("data");
const TOTAL_SENT_PATH = path.join(STATE_PATH, "total_sent.txt");
const LAST_RECEIVER_PATH = path.join(STATE_PATH, "last_receiver.txt");
const LAST_SENDER_PATH = path.join(STATE_PATH, "last_sender.txt");

function readStateFile(filePath) {
  return fs.existsSync(filePath) ? fs.readFileSync(filePath, "utf8").trim() : "";
}

function writeStateFile(filePath, content) {
  fs.writeFileSync(filePath, content, "utf8");
}

function fetchHtmlTemplate() {
  return fs.readFileSync(HTML_TEMPLATE_PATH, "utf8");
}

async function downloadS3FileToString(key) {
  const result = await s3.getObject({ Bucket: BUCKET_NAME, Key: key }).promise();
  return result.Body.toString("utf-8");
}

async function loadUnsubscribedSet() {
  const data = await downloadS3FileToString(UNSUBSCRIBED_KEY);
  return new Set(
    data
      .split("\n")
      .map((e) => e.trim().toLowerCase())
      .filter(Boolean)
  );
}

function nextSender(lastSender) {
  const index = SENDERS.indexOf(lastSender);
  return SENDERS[(index + 1) % SENDERS.length] || SENDERS[0];
}

async function sendEmail(source, to, subject, html) {
  try {
    const params = {
      Source: source,
      Destination: { ToAddresses: [to] },
      Message: {
        Subject: { Data: subject },
        Body: { Html: { Data: html } },
      },
      ConfigurationSetName: CONFIG_SET,
    };
    const result = await ses.sendEmail(params).promise();
    logger.info(`Sent email to ${to} MessageId=${result.MessageId}`);
    return true;
  } catch (err) {
    logger.error(`Error sending email to ${to}`, err);
    return false;
  }
}

async function buildBatch(unsubSet, lastReceiver, limit) {
  const data = await downloadS3FileToString(EMAIL_CSV_KEY);
  const lines = data.split("\n");
  const headers = lines[0].split(",").map((h) => h.trim());
  const emailIdx = headers.indexOf("Email");
  const usernameIdx = headers.indexOf("Username");

  const recipients = [];
  let seen = !lastReceiver;

  for (let i = 1; i < lines.length && recipients.length < limit; i++) {
    const values = lines[i].split(",");
    const email = values[emailIdx]?.trim();
    const username = values[usernameIdx]?.trim();

    if (!email || !username || unsubSet.has(email.toLowerCase())) continue;

    if (!seen) {
      if (email === lastReceiver) seen = true;
      continue;
    }

    recipients.push({ email, username });
  }

  return recipients;
}

app.post("/run-campaign", async (req, res) => {
  logger.info("Campaign start");

  const unsubSet = await loadUnsubscribedSet();
  const totalSent = parseInt(readStateFile(TOTAL_SENT_PATH) || "0", 10);
  const lastReceiver = readStateFile(LAST_RECEIVER_PATH);
  const lastSender = readStateFile(LAST_SENDER_PATH);
  const remaining = MAX_TOTAL_EMAILS - totalSent;

  if (remaining <= 0) {
    logger.info("Campaign already completed.");
    return res.status(200).send("Campaign completed.");
  }

  const htmlTemplate = fetchHtmlTemplate();
  const batch = await buildBatch(unsubSet, lastReceiver, remaining);
  logger.info(`Sending batch of ${batch.length}`);

  let sentCount = 0;
  let sender = nextSender(lastSender);
  const start = Date.now();

  for (const { email, username } of batch) {
    const elapsed = (Date.now() - start) / 1000;
    if (elapsed > 720) break; // stop at 12 minutes

    const unsubscribeURL = `https://lpv4lifyk9.execute-api.eu-west-1.amazonaws.com/Unsubscribefunction?email=${encodeURIComponent(email)}`;
    const html = htmlTemplate
      .replace("&#64;{{INSTAHANDLE}}", `&#64;${username}`)
      .replace("{{URL}}", unsubscribeURL);

    const ok = await sendEmail(sender, email, SUBJECT, html);
    if (ok) {
      sentCount++;
      writeStateFile(LAST_RECEIVER_PATH, email);
      writeStateFile(LAST_SENDER_PATH, sender);
      sender = nextSender(sender);
      await new Promise((r) => setTimeout(r, Math.random() * 300 + 200)); // 0.2–0.5s
    }
  }

  writeStateFile(TOTAL_SENT_PATH, (totalSent + sentCount).toString());

  const adjusted = totalSent + sentCount + unsubSet.size;
  logger.info(`Campaign progress: ${adjusted}/${MAX_TOTAL_EMAILS}`);

  const subject = adjusted >= MAX_TOTAL_EMAILS ? "✅ Email Batch Complete" : "⏳ Batch Incomplete";
  const body = `<p>Sent=${sentCount}, Total=${totalSent + sentCount}, Unsub=${unsubSet.size}, Adjusted=${adjusted}</p>`;
  await sendEmail(sender, NOTIFY_EMAIL, subject, body);

  res.status(200).send("Campaign batch completed");
});

app.listen(process.env.PORT || 3000, () => {
  console.log(`🚀 Server running on port ${process.env.PORT || 3000}`);
});