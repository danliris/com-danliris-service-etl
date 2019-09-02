let sqlConnections = require('./src');

let moment = require('moment');

module.exports = async function (context, req) {
    context.log('JavaScript HTTP trigger function processed a request.');

    await sqlConnections
        .sqlDWH
        .query(`SELECT 'A' AS Name`, {
            type: sqlConnections.sqlDWH.QueryTypes.SELECT
        })
        .then((result) => {
            context.res = {
                body: JSON.stringify(result)
            };
        })
        .catch((e) => {
            context.res = {
                body: JSON.stringify(e)
            }
        });
    // if (req.query.name || (req.body && req.body.name)) {
    //     context.res = {
    //         // status: 200, /* Defaults to 200 */
    //         body: process.env["ConnectionStrings"].toString()
    //     };
    // }
    // else {
    //     context.res = {
    //         status: 400,
    //         body: "Please pass a name on the query string or in the request body"
    //     };
    // }
};