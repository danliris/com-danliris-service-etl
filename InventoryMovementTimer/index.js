let run = require('../InventoryMovement/run');

module.exports = async function (context, myTimer) {
    var timeStamp = new Date().toISOString();
    
    if (myTimer.IsPastDue)
    {
        context.log('JavaScript is running late!');
    }
    context.log('JavaScript timer trigger function ran!', timeStamp);   
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