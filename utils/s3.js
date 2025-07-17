const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const { S3_BUCKET } = process.env;

exports.getObject = async (Key) => {
  const res = await s3.getObject({ Bucket: S3_BUCKET, Key }).promise();
  return res.Body.toString('utf-8');
};

exports.uploadObject = async (Key, data) => {
  return s3.putObject({
    Bucket: S3_BUCKET,
    Key,
    Body: data,
    ContentType: 'text/plain',
  }).promise();
};
