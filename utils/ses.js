const AWS = require('aws-sdk');
const ses = new AWS.SES();
const { SES_TEMPLATE } = process.env;

exports.sendTemplateEmail = async ({ to, templateData, from }) => {
  return ses.sendTemplatedEmail({
    Source: from,
    Destination: { ToAddresses: [to] },
    Template: SES_TEMPLATE,
    TemplateData: JSON.stringify(templateData)
  }).promise();
};
