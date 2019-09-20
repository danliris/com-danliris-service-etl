let sqlDWHConnections = require('../Connection/DWH/');
let sqlDealConnections = require('../Connection/Deal/')
const MIGRATION_LOG_DESCRIPTION = 'Fact Deal Tracking Deal from MongoDB to Azure DWH';
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
};

const extract = async function (times) {
    var time = times.length > 0 ? moment(times[0].start).format("YYYY-MM-DD") : "1970-01-01";
    var timestamp = new Date(time);
    return await sqlDealConnections
        .sqlDeal
        .query(`select 
        d.IsDeleted _deleted,
        d.id _id,
        d.code,
        d.createdutc _createdDate,
        d.CreatedBy _createdBy,
        d.name,
        d.amount,
        d.companyCode,
        d.companyName,
        c.City companyCity,
        d.contactCode,
        d.contactName,
        d.closeDate,
        d.description,
        d.reason,
        d.quantity,
        d.uomUnit
        from DealTrackingDeals d left join Companies c on d.CompanyId = c.Id
        where d.lastmodifiedutc > ?
        `, {
            replacements: [timestamp],
            type: sqlDealConnections.sqlDeal.QueryTypes.SELECT
        });

};

function transform(data) {
    var results = data.map((item) => {
        return {
            deleted: `'${item._deleted}'`,
            id: `'${item._id.toString()}'`,
            code: item.code ? `'${item.code.replace(/'/g, '"')}'` : null,
            createdDate: `'${moment(item._createdDate).add(7, "hours").format("YYYY-MM-DD")}'`,
            createdBy: `'${item._createdBy}'`,
            name: item.name ? `'${item.name.replace(/'/g, '"')}'` : null,
            amount: `'${item.amount}'`,
            companyCode: item.companyCode ? `'${item.companyCode.replace(/'/g, '"')}'` : null,
            companyName: item.companyName ? `'${item.companyName.replace(/'/g, '"')}'` : null,
            companyCity: item.companyCity ? `'${item.companyCity.replace(/'/g, '"')}'` : null,
            contactCode: item.contactCode ? `'${item.contactCode.replace(/'/g, '"')}'` : null,
            contactName: item.contactName ? `'${item.contactName.replace(/'/g, '"')} ${item.contact.lastName.replace(/'/g, '"')}'` : null,
            closeDate: item.closeDate ? `'${moment(item.closeDate).add(7, "hours").format("YYYY-MM-DD")}'` : null,
            description: item.description ? `'${item.description.replace(/'/g, '"')}'` : null,
            reason: item.reason ? `'${item.reason.replace(/'/g, '"')}'` : null,
            // productCode: item.product ? `'${item.product.code.replace(/'/g, '"')}'` : null,
            // productName: item.product ? `'${item.product.name.replace(/'/g, '"')}'` : null,
            productCode: null,
            productName: null,

            quantity: item.quantity != undefined ? `'${item.quantity}'` : null,
            uom: item.uomUnit ? `'${item.uomUnit.replace(/'/g, '"')}'` : null
        };
    });
    return Promise.resolve([].concat.apply([], results));
}

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
                var sqlQuery = 'INSERT INTO [DL_Fact_Deal_Tracking_Deal_Temp](deleted, id, code, createdDate, createdBy, name, amount, companyCode, companyName, contactCode, contactName, closeDate, description, reason, companyCity, productCode, productName, quantity, UOM) ';

                var count = 1;
                for (var item of data) {
                    if (item) {
                        var values = `${item.deleted}, ${item.id}, ${item.code}, ${item.createdDate}, ${item.createdBy}, ${item.name}, ${item.amount}, ${item.companyCode}, ${item.companyName}, ${item.contactCode}, ${item.contactName}, ${item.closeDate}, ${item.description}, ${item.reason}, ${item.companyCity}, ${item.productCode}, ${item.productName}, ${item.quantity}, ${item.uom}`;
                        var queryString = `\nSELECT ${values} UNION ALL `;

                        sqlQuery = sqlQuery.concat(queryString);

                        if (count % 4000 == 0) {
                            sqlQuery = sqlQuery.substring(0, sqlQuery.length - 10);
                            command.push(insertQuery(sqlDWHConnections.sqlDWH, sqlQuery, t));
                            sqlQuery = "INSERT INTO [DL_Fact_Deal_Tracking_Deal_Temp](deleted, id, code, createdDate, createdBy, name, amount, companyCode, companyName, contactCode, contactName, closeDate, description, reason, companyCity, productCode, productName, quantity, UOM) ";
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
                        sqlDWHConnections.sqlDWH.query("exec [DL_Upsert_Fact_Deal_Tracking_Deal]", {
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