const winston = require('winston');
require('winston-cloudwatch');

const { LOG_GROUP, LOG_STREAM, AWS_REGION } = process.env;

// Logger
exports.logger = winston.createLogger({
    transports: [
      new winston.transports.Console(),
      new winston.transports.CloudWatch({
        logGroupName: LOG_GROUP,
        logStreamName: LOG_STREAM,
        awsRegion: AWS_REGION,
      })
    ]
  });