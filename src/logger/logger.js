const winston = require("winston");
require("winston-cloudwatch");

const logger = winston.createLogger({
  transports: [
    new winston.transports.Console(),
    new winston.transports.CloudWatch({
      logGroupName: "/ec2/email-campaign",
      logStreamName: "email-campaign-stream",
      awsRegion: process.env.AWS_REGION || "eu-west-1",
    }),
  ],
});

module.exports = logger;
