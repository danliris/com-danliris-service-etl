let sqlDWHConnections = require('../Connection/DWH/');
let sqlFPConnection = require('../Connection/FinishingPrinting/')

let moment = require('moment');

module.exports = async function (context, req) {    
    context.log('JavaScript HTTP trigger function processed a request.');

    await sqlFPConnection
        .sqlFP
        .query(`SELECT Top(1) * From Kanbans`, {
            type: sqlFPConnection.sqlFP.QueryTypes.SELECT
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