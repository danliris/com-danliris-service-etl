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
        });
};