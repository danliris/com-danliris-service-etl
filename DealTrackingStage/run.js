let sqlDWHConnections = require('../Connection/DWH/');
let sqlDealConnections = require('../Connection/Deal/')
const MIGRATION_LOG_DESCRIPTION = 'Fact Deal Tracking Stage from MongoDB to Azure DWH';
let moment = require('moment');

module.exports = async function () {
    var startedDate = new Date();
    return await timestamp()
        .then((times) => joinDealTracking(times))
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

const extractDealTrackingStage = function (times) {
    var time = times.length > 0 ? moment(times[0].start).format("YYYY-MM-DD") : "1970-01-01";
    var timestamp = new Date(time);
    return sqlDealConnections
        .sqlDeal
        .query(`select
        IsDeleted _deleted,
        id _id,
        code,
        createdby _createdBy,
        createdutc _createdDate,
        boardId,
        name
        from dealtrackingstages
        where lastmodifiedutc > ?
        `, {
            replacements: [timestamp],
            type: sqlDealConnections.sqlDeal.QueryTypes.SELECT
        });

};

const extractDealTrackingStageDeals = function (times) {
    var time = times.length > 0 ? moment(times[0].start).format("YYYY-MM-DD") : "1970-01-01";
    var timestamp = new Date(time);
    return sqlDealConnections
        .sqlDeal
        .query(`select
        stageId,
        id
        from DealTrackingDeals
        where lastmodifiedutc > ?
        `, {
            replacements: [timestamp],
            type: sqlDealConnections.sqlDeal.QueryTypes.SELECT
        });

};

const joinDealTracking = function (times) {
    var dealStages = extractDealTrackingStage(times);
    var dealDeals = extractDealTrackingStageDeals(times);

    return Promise.all([dealStages, dealDeals])
        .then((data) => {
            var dealStages = data[0];
            var dealDeals = data[1];

            var dealTracking = {};
            dealTracking.Stages = dealStages;
            dealTracking.StageDeals = dealDeals;

            return Promise.resolve(dealTracking);
        });
};

function transform(data) {
    var results = data.Stages.map((item) => {
        return {
            deleted: `'${item._deleted}'`,
            id: `'${item._id.toString()}'`,
            code: item.code ? `'${item.code.replace(/'/g, '"')}'` : null,
            createdDate: `'${moment(item._createdDate).add(7, "hours").format("YYYY-MM-DD")}'`,
            createdBy: `'${item._createdBy}'`,
            boardId: `'${item.boardId.toString()}'`,
            name: item.name ? `'${item.name.replace(/'/g, '"')}'` : null,
        };
    });
    results = [].concat.apply([], results);

    var resultMap = data.StageDeals.map((item) => {
        return {
            stageId: `'${stageId.toString()}'`,
            dealId: `'${id.toString()}'`
        }
    });

    resultMap = [].concat.apply([], resultMap);

    var trf = {
        stages: results,
        deals: resultMap
    };

    return Promise.resolve(trf);
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
                var sqlQuery = 'INSERT INTO [DL_Fact_Deal_Tracking_Stage_Temp](deleted, id, code, createdDate, createdBy, boardId, name) ';

                var count = 1;
                for (var item of data.stages) {
                    if (item) {
                        var values = `${item.deleted}, ${item.id}, ${item.code}, ${item.createdDate}, ${item.createdBy}, ${item.boardId}, ${item.name}`;
                        var queryString = `\nSELECT ${values} UNION ALL `;

                        sqlQuery = sqlQuery.concat(queryString);

                        if (count % 4000 == 0) {
                            sqlQuery = sqlQuery.substring(0, sqlQuery.length - 10);
                            command.push(insertQuery(sqlDWHConnections.sqlDWH, sqlQuery, t));
                            sqlQuery = "INSERT INTO [DL_Fact_Deal_Tracking_Stage_Temp](deleted, id, code, createdDate, createdBy, title, boardId, name) ";
                        }
                        console.log(`add data to query  : ${count}`);
                        count++;
                    }
                }
                var sqlQueryMap = 'INSERT INTO [DL_Fact_Deal_Tracking_Stage_Deal_Temp](stageId, dealId) ';
                var countMap = 1;

                for (var deal of data.deals) {
                    if (deal) {
                        var valuesMap = `${deal.stageId}, ${deal.dealId}`;
                        var queryStringMap = `\nSELECT ${valuesMap} UNION ALL `;
                        
                        sqlQueryMap = sqlQueryMap.concat(queryStringMap);
                        if (countMap % 4000 === 0) {
                            sqlQueryMap = sqlQueryMap.substring(0, sqlQueryMap.length - 10);
                            command.push(insertQuery(sqlDWHConnections.sqlDWH, sqlQueryMap, t));
                            sqlQueryMap = "INSERT INTO [DL_Fact_Deal_Tracking_Stage_Deal_Temp](stageId, dealId) ";
                        }
                        console.log(`add data map to query  : ${countMap}`);
                        countMap++;
                    }
                }

                if (sqlQuery != "") {
                    sqlQuery = sqlQuery.substring(0, sqlQuery.length - 10);
                    command.push(insertQuery(sqlDWHConnections.sqlDWH, `${sqlQuery}`, t));
                }

                if (sqlQueryMap !== "") {
                    sqlQueryMap = sqlQueryMap.substring(0, sqlQueryMap.length - 10);
                    command.push(insertQuery(sqlDWHConnections.sqlDWH, `${sqlQueryMap}`, t));
                }


                return Promise.all(command)
                    .then((results) => {
                        sqlDWHConnections.sqlDWH.query("exec [DL_Upsert_Fact_Deal_Tracking_Stage]", {
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