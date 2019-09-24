let sqlDWHConnections = require('../Connection/DWH/');
let sqlFPConnection = require('../Connection/FinishingPrinting/')
let sqlCoreConnection = require('../Connection/Core/')
let sqlSalesConnection = require('../Connection/Sales/')

let run = require('./run');

module.exports = async function (context, req) {
    context.log('JavaScript HTTP trigger function processed a request.');

    await run()
        .then((result) => {
            console.log('success');
            context.res = {
                body: JSON.stringify(result)
            };
        })
        .catch((e) => {
            context.res = {
                body: JSON.stringify(e)
            }
        });;

};

