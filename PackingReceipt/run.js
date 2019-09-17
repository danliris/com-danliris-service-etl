let sqlDWHConnections = require('../Connection/DWH/');
let sqlFPConnection = require('../Connection/FinishingPrinting/')
const MIGRATION_LOG_DESCRIPTION = "Fact FP Packing Receipt from MongoDB to Azure DWH";
let moment = require('moment');

module.exports = async function () {
    var startedDate = new Date();
    return await timestamp()
        .then((times) => joinPackingReceipt(times))
        .then((data) => transform(data))
        .then((data) => load(data))
        .then(async (results) => {
            var finishedDate = new Date();
            var spentTime = moment(finishedDate).diff(moment(startedDate), "minutes");
            var updateLog = {
                description: MIGRATION_LOG_DESCRIPTION,
                start: startedDate,
                finish: finishedDate,
                executionTime: spentTime + " minutes",
                status: "Successful"
            };
            return await updateMigrationLog(updateLog);
        })
        .catch(async (err) => {
            var finishedDate = new Date();
            var spentTime = moment(finishedDate).diff(moment(startedDate), "minutes");
            var updateLog = {
                description: MIGRATION_LOG_DESCRIPTION,
                start: startedDate,
                finish: finishedDate,
                executionTime: spentTime + " minutes",
                status: err
            };
            return await updateMigrationLog(updateLog);
        });

}

async function timestamp() {
    return await sqlDWHConnections
        .sqlDWH
        .query(`select top(1) * from [migration-log]
        where description = ? and status = 'Successful' 
        order by finish desc`, {
            replacements: [MIGRATION_LOG_DESCRIPTION],
            type: sqlDWHConnections.sqlDWH.QueryTypes.SELECT
        });
}

async function updateMigrationLog(log) {
    return await sqlDWHConnections
        .sqlDWH
        .query(`insert into [dbo].[migration-log](description, start, finish, executionTime, status)
        values('${log.description}', '${moment(log.start).format("YYYY-MM-DD HH:mm:ss")}', '${moment(log.finish).format("YYYY-MM-DD HH:mm:ss")}', '${log.executionTime}', '${log.status}')`)
        .then(([results, metadata]) => {
            return metadata;
        })
        .catch((e) => {
            return e;
        });
}

const extractPR = async function (times) {
    var time = times.length > 0 ? times[0].start : "1970-01-01";
    var timestamp = new Date(time);
    return sqlFPConnection
        .sqlFP
        .query(`select
        id,
        IsDeleted _deleted,
        code,
        date,
        packingCode,
        accepted,
        declined,
        referenceNo,
        referenceType,
        type,
        productionOrderNo,
        buyer,
        colorName,
        construction,
        packingUom,
        orderType,
        colorType,
        designCode,
        designNumber
        from packingreceipt
        where lastmodifiedutc > ?`, {
            replacements: [timestamp],
            type: sqlFPConnection.sqlFP.QueryTypes.SELECT
        });

};

const getPackingReceiptItems = async function () {
    return sqlFPConnection
        .sqlFP
        .query(`select
        id,
        product,
        quantity,
        length,
        weight,
        packingReceiptId
        from packingreceiptItem `, {
            type: sqlFPConnection.sqlFP.QueryTypes.SELECT
        });

};

const joinPackingReceipt = function (times) {
    var packingReceipt = extractPR(times);
    var packingReceiptItems = getPackingReceiptItems();

    return Promise.all([packingReceipt, packingReceiptItems])
        .then((data) => {
            var packingReceipt = data[0];
            var packingReceiptItems = data[1];

            for (var element of packingReceipt) {
                element.items = packingReceiptItems.filter(x => x.packingReceiptId == element.id);
            }

            return Promise.resolve(packingReceipt);
        });
}

function transform(data) {
    var results = data.map((fpPackingReceipt) => {
        var fpPackingReceiptItems = fpPackingReceipt.items && fpPackingReceipt.items.length > 0 ? fpPackingReceipt.items : null;

        if (fpPackingReceiptItems) {
            var items = fpPackingReceiptItems.map((item) => {
                return {
                    deleted: `'${fpPackingReceipt._deleted}'`,
                    code: fpPackingReceipt.code ? `'${fpPackingReceipt.code}'` : null,
                    date: fpPackingReceipt.date ? `'${moment(fpPackingReceipt.date).add(7, "hours").format("YYYY-MM-DD")}'` : null,
                    packingCode: fpPackingReceipt.packingCode ? `'${fpPackingReceipt.packingCode}'` : null,
                    accepted: `'${fpPackingReceipt.accepted}'`,
                    declined: `'${fpPackingReceipt.declined}'`,
                    referenceNo: fpPackingReceipt.referenceNo ? `'${fpPackingReceipt.referenceNo}'` : null,
                    referenceType: fpPackingReceipt.referenceType ? `'${fpPackingReceipt.referenceType}'` : null,
                    type: fpPackingReceipt.type ? `'${fpPackingReceipt.type.replace(/'/g, '"')}'` : null,
                    productionOrderNo: fpPackingReceipt.productionOrderNo ? `'${fpPackingReceipt.productionOrderNo}'` : null,
                    buyer: fpPackingReceipt.buyer ? `'${fpPackingReceipt.buyer.replace(/'/g, '"')}'` : null,
                    colorName: fpPackingReceipt.colorName ? `'${fpPackingReceipt.colorName.replace(/'/g, '"')}'` : null,
                    construction: fpPackingReceipt.construction ? `'${fpPackingReceipt.construction.replace(/'/g, '"')}'` : null,
                    packingUom: fpPackingReceipt.packingUom ? `'${fpPackingReceipt.packingUom.replace(/'/g, '"')}'` : null,
                    orderType: fpPackingReceipt.orderType ? `'${fpPackingReceipt.orderType.replace(/'/g, '"')}'` : null,
                    colorType: fpPackingReceipt.colorType ? `'${fpPackingReceipt.colorType.replace(/'/g, '"')}'` : null,
                    designCode: fpPackingReceipt.designCode ? `'${fpPackingReceipt.designCode}'` : null,
                    designNumber: fpPackingReceipt.designNumber ? `'${fpPackingReceipt.designNumber.replace(/'/g, '"')}'` : null,
                    product: item.product ? `'${item.product.replace(/'/g, '"')}'` : null,
                    quantity: item.quantity ? `'${item.quantity}'` : null,
                    length: item.length ? `'${item.length}'` : null,
                    weight: item.weight ? `'${item.weight}'` : null
                }
            });

            return [].concat.apply([], items);
        }
    });
    return Promise.resolve([].concat.apply([], results));
};

function insertQuery(sql, query, transaction) {
    return new Promise((resolve, reject) => {
        sql.query(query, {
            transaction: transaction
        })
            .then(([results, metadata]) => {
                resolve(metadata);
            })
            .catch((e) => {
                reject(e);
            });
    });
};

function load(data) {
    return new Promise((resolve, reject) => {
        sqlDWHConnections
            .sqlDWH
            .transaction()
            .then(t => {
                var command = [];
                var sqlQuery = 'INSERT INTO [dbo].[DL_Fact_FPPackingReceipt_Temp]([deleted], [code], [date], [packingCode], [accepted], [declined], [referenceNo], [referenceType], [type], [productionOrderNo], [buyer], [colorName], [construction], [packingUOM], [orderType], [colorType], [designCode], [designNumber], [product], [quantity], [length], [weight]) ';

                var count = 1;
                for (var item of data) {
                    if (item) {
                        var queryString = `\nSELECT ${item.deleted}, ${item.code}, ${item.date}, ${item.packingCode}, ${item.accepted}, ${item.declined}, ${item.referenceNo}, ${item.referenceType}, ${item.type}, ${item.productionOrderNo}, ${item.buyer}, ${item.colorName}, ${item.construction}, ${item.packingUom}, ${item.orderType}, ${item.colorType}, ${item.designCode}, ${item.designNumber}, ${item.product}, ${item.quantity}, ${item.length}, ${item.weight} UNION ALL `;
                        sqlQuery = sqlQuery.concat(queryString);
                        if (count % 1000 == 0) {
                            sqlQuery = sqlQuery.substring(0, sqlQuery.length - 10);
                            command.push(insertQuery(sqlDWHConnections.sqlDWH, sqlQuery, t));
                            sqlQuery = "INSERT INTO [dbo].[DL_Fact_FPPackingReceipt_Temp]([deleted], [code], [date], [packingCode], [accepted], [declined], [referenceNo], [referenceType], [type], [productionOrderNo], [buyer], [colorName], [construction], [packingUOM], [orderType], [colorType], [designCode], [designNumber], [product], [quantity], [length], [weight]) ";
                        }
                        console.log(`add data to query  : ${count}`);
                        count++;
                    }
                }


                if (sqlQuery != "") {
                    sqlQuery = sqlQuery.substring(0, sqlQuery.length - 10);
                    command.push(insertQuery(sqlDWHConnections.sqlDWH, `${sqlQuery}`, t));
                }


                return Promise.all(command)
                    .then((results) => {
                        sqlDWHConnections.sqlDWH.query("exec [DL_Upsert_Fact_FPPackingReceipt]", {
                            transaction: t
                        }).then((execResult) => {
                            t.commit()
                                .then(() => {
                                    resolve(results);
                                })
                                .catch((err) => {
                                    reject(err);
                                });


                        }).catch((error) => {
                            t.rollback()
                                .then(() => {
                                    reject(error);
                                })
                                .catch((err) => {
                                    reject(err);
                                });
                        });
                    })
                    .catch((error) => {
                        t.rollback()
                            .then(() => {
                                reject(error);
                            })
                            .catch((err) => {
                                reject(err);
                            });
                    });
            })
            .catch((err) => {
                reject(err);
            });
    })
        .catch((err) => {
            reject(err);
        });
};