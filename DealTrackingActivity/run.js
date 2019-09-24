let sqlDWHConnections = require('../Connection/DWH/');
let sqlDealConnections = require('../Connection/Deal/')
const MIGRATION_LOG_DESCRIPTION = 'Fact Deal Tracking Activity from MongoDB to Azure DWH';
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
        IsDeleted _deleted,
        id _id,
        code,
        createdby _createdBy,
        createdutc _createdDate,
        dealId,
        type,
        notes,
        taskTitle,
        dueDate,
        status,
        stagefromid sourceStageId,
        stagetoid targetStageId,
        assignedTo
        from dealtrackingactivities
        where lastmodifiedutc > ?
        `, {
            replacements: [timestamp],
            type: sqlDealConnections.sqlDeal.QueryTypes.SELECT
        });

};

function transform(data) {
    var result = data.map((item) => {
        return {
            deleted: `'${item._deleted}'`,
            id: `'${item._id.toString()}'`,
            code: item.code ? `'${item.code.replace(/'/g, '"')}'` : null,
            createdDate: `'${moment(item._createdDate).add(7, "hours").format("YYYY-MM-DD HH:mm:ss")}'`,
            createdBy: `'${item._createdBy}'`,
            dealId: item.dealId ? `'${item.dealId.toString()}'` : null,
            type: item.type ? `'${item.type.replace(/'/g, '"')}'` : null,
            notes: item.notes ? `'${item.notes.replace(/'/g, '"')}'` : null,
            title: item.title ? `'${item.title.replace(/'/g, '"')}'` : null,
            dueDate: item.dueDate ? `'${moment(item.dueDate).add(7, "hours").format("YYYY-MM-DD")}'` : null,
            status: item.status != undefined ? `'${item.status}'` : null,
            sourceStageId: item.sourceStageId ? `'${item.sourceStageId.toString().replace(/'/g, '"')}'` : null,
            targetStageId: item.targetStageId ? `'${item.targetStageId.toString().replace(/'/g, '"')}'` : null,
            assignedTo: item.assignedTo ? `'${item.assignedTo.replace(/'/g, '"')}'` : null
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
                var sqlQuery = 'INSERT INTO [DL_Fact_Deal_Tracking_Activity_Temp](deleted, id, code, createdDate, createdBy, dealId, type, notes, title, dueDate, status, sourceStageId, targetStageId, assignedTo) ';

                var count = 1;
                for (var item of data) {
                    if (item) {
                        var values = `${item.deleted}, ${item.id}, ${item.code}, ${item.createdDate}, ${item.createdBy}, ${item.dealId}, ${item.type}, ${item.notes}, ${item.title}, ${item.dueDate}, ${item.status}, ${item.sourceStageId}, ${item.targetStageId}, ${item.assignedTo}`;
                        var queryString = `\nSELECT ${values} UNION ALL `;

                        sqlQuery = sqlQuery.concat(queryString);

                        if (count % 4000 == 0) {
                            sqlQuery = sqlQuery.substring(0, sqlQuery.length - 10);
                            command.push(insertQuery(sqlDWHConnections.sqlDWH, sqlQuery, t));
                            sqlQuery = "INSERT INTO [DL_Fact_Deal_Tracking_Activity_Temp](deleted, id, code, createdDate, createdBy, dealId, type, notes, title, dueDate, status, sourceStageId, targetStageId, assignedTo) ";
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
                        sqlDWHConnections.sqlDWH.query("exec [DL_Upsert_Fact_Deal_Tracking_Activity]", {
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