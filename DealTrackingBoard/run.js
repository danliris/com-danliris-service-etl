let sqlDWHConnections = require('../Connection/DWH/');
let sqlDealConnections = require('../Connection/Deal/');
let sqlCoreConnections = require('../Connection/Core/');
const MIGRATION_LOG_DESCRIPTION = 'Fact Deal Tracking Board from MongoDB to Azure DWH';
let moment = require('moment');

module.exports = async function () {
    var startedDate = new Date();
    return await timestamp()
        .then((times) => joinDealCurrencies(times))
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

const extractDealBoard = function (times) {
    var time = times.length > 0 ? moment(times[0].start).format("YYYY-MM-DD") : "1970-01-01";
    var timestamp = new Date(time);
    return sqlDealConnections
        .sqlDeal
        .query(`select 
        IsDeleted _deleted,
        id _id,
        code,
        createdutc _createdDate,
        CreatedBy _createdBy,
        title,
        currencyCode,
        currencySymbol
        from DealTrackingBoards
        where lastmodifiedutc > ?
        `, {
            replacements: [timestamp],
            type: sqlDealConnections.sqlDeal.QueryTypes.SELECT
        });

};

const extractCurrencies = function () {
    return sqlCoreConnections
        .sqlCore
        .query(`select
        code,
        rate
        from currencies`, {
            type: sqlCoreConnections.sqlCore.QueryTypes.SELECT
        });
}

const joinDealCurrencies = function (times) {
    var dealBoard = extractDealBoard(times);
    var currencies = extractCurrencies();

    return Promise.all([dealBoard, currencies])
        .then((data) => {
            var dealBoard = data[0];
            var currencies = data[1];

            for (var element of dealBoard) {
                var currency = currencies.find(x => x.code == element.currencyCode);
                if (currency) {
                    element.currencyRate = currency.rate;
                }
            }

            return Promise.resolve(dealBoard);
        });
}

function transform(data) {
    var result = data.map((item) => {
        return {
            deleted: `'${item._deleted}'`,
            id: `'${item._id.toString()}'`,
            code: item.code ? `'${item.code.replace(/'/g, '"')}'` : null,
            createdDate: `'${moment(item._createdDate).add(7, "hours").format("YYYY-MM-DD")}'`,
            createdBy: `'${item._createdBy}'`,
            title: item.title ? `'${item.title.replace(/'/g, '"')}'` : null,
            currencyCode: item.currencyCode ? `'${item.currencyCode.replace(/'/g, '"')}'` : null,
            currencyRate: item.currencyRate ? `'${item.currencyRate}'` : null,
            currencySymbol: item.currencySymbol ? `'${item.currencySymbol.replace(/'/g, '"')}'` : null
        };
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
                var sqlQuery = 'INSERT INTO [DL_Fact_Deal_Tracking_Board_Temp](deleted, id, code, createdDate, createdBy, title, currencyCode, currencyRate, currencySymbol) ';

                var count = 1;
                for (var item of data) {
                    if (item) {
                        var values = `${item.deleted}, ${item.id}, ${item.code}, ${item.createdDate}, ${item.createdBy}, ${item.title}, ${item.currencyCode}, ${item.currencyRate}, ${item.currencySymbol}`;
                        var queryString = `\nSELECT ${values} UNION ALL `;

                        sqlQuery = sqlQuery.concat(queryString);

                        if (count % 4000 == 0) {
                            sqlQuery = sqlQuery.substring(0, sqlQuery.length - 10);
                            command.push(insertQuery(sqlDWHConnections.sqlDWH, sqlQuery, t));
                            sqlQuery = "INSERT INTO [DL_Fact_Deal_Tracking_Board_Temp](deleted, id, code, createdDate, createdBy, title, currencyCode, currencyRate, currencySymbol) ";
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
                        sqlDWHConnections.sqlDWH.query("exec [DL_Upsert_Fact_Deal_Tracking_Board]", {
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