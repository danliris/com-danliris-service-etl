let sqlDWHConnections = require('../Connection/DWH/');
let sqlInventoryConnection = require('../Connection/Inventory/')
const MIGRATION_LOG_DESCRIPTION = "Fact Inventory Movement from MongoDB to Azure DWH";
let moment = require('moment');

module.exports = async function () {
    var startedDate = new Date();
    return await timestamp()
        .then((times) => extract(times))
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

};

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

const extract = async function (times) {
    var time = times.length > 0 ? moment(times[0].start).format("YYYY-MM-DD") : "1970-01-01";
    var timestamp = new Date(time);
    return await sqlInventoryConnection
        .sqlInventory
        .query(`select
        storageCode,
        storageName,
        date,
        quantity,
        type,
        productCode,
        productName,
        uomunit uom,
        _IsDeleted _deleted,
        referenceNo,
        referenceType,
        before,
        after,
        remark,
        no code
        from inventorymovements
        where _lastmodifiedutc > ?`, {
            replacements: [timestamp],
            type: sqlInventoryConnection.sqlInventory.QueryTypes.SELECT
        });

};

function transform(data) {
    var result = data.map((item) => {
        var date = moment(item.date).add(7, "hours").format("YYYY-MM-DD");

        return {
            storageCode: item.storageCode ? `'${item.storageCode.replace(/'/g, '"')}'` : null,
            storageName: item.storageName ? `'${item.storageName.replace(/'/g, '"')}'` : null,
            date: `'${date}'`,
            qty: item.quantity,
            status: item.type ? `'${item.type}'` : null,
            productCode: item.productCode ? `'${item.productCode.replace(/'/g, '"')}'` : null,
            productName: item.productName ? `'${item.productName.replace(/'/g, '"')}'` : null,
            uom: item.uom ? `'${item.uom.replace(/'/g, '"')}'` : null,
            deleted: `'${item._deleted}'`,
            code: item.code ? `'${item.code.replace(/'/g, '"')}'` : null,
            referenceNo: item.referenceNo ? `'${item.referenceNo.replace(/'/g, '"')}'` : null,
            referenceType: item.referenceType ? `'${item.referenceType.replace(/'/g, '"')}'` : null,
            before: item.before,
            after: item.after,
            remark: item.remark ? `'${item.remark.replace(/'/g, '"')}'` : null,
        }
    });
    return Promise.resolve([].concat.apply([], result));
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
                var sqlQuery = 'INSERT INTO [DL_Fact_Inventory_Movement_Temp]([Storage Code], [Date], [Quantity], [Status], [Product Code], [UOM], [Deleted], [Code], [ReferenceNo], [ReferenceType], [Before], [After], [Remark], [Storage Name], [Product Name]) ';

                var count = 1;
                for (var item of data) {
                    if (item) {
                        var values = `${item.storageCode}, ${item.date}, ${item.qty}, ${item.status}, ${item.productCode}, ${item.uom}, ${item.deleted}, ${item.code}, ${item.referenceNo}, ${item.referenceType}, ${item.before}, ${item.after}, ${item.remark}, ${item.storageName}, ${item.productName}`
                        var queryString = `\nSELECT ${values} UNION ALL `;
                        sqlQuery = sqlQuery.concat(queryString);
                        if (count % 1000 == 0) {
                            sqlQuery = sqlQuery.substring(0, sqlQuery.length - 10);
                            command.push(insertQuery(sqlDWHConnections.sqlDWH, sqlQuery, t));
                            sqlQuery = "INSERT INTO [DL_Fact_Inventory_Movement_Temp]([Storage Code], [Date], [Quantity], [Status], [Product Code], [UOM], [Deleted], [Code], [ReferenceNo], [ReferenceType], [Before], [After], [Remark], [Storage Name], [Product Name]) ";
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
                        sqlDWHConnections.sqlDWH.query("exec [DL_UPSERT_FACT_INVENTORY_MOVEMENT]", {
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