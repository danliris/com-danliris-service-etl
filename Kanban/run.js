let sqlDWHConnections = require('../Connection/DWH/');
let sqlFPConnection = require('../Connection/FinishingPrinting/')
const MIGRATION_LOG_DESCRIPTION = "Fact Kanban from MongoDB to Azure DWH";
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
};

function extract(times) {
    var time = times.length > 0 ? moment(times[0].start).format("YYYY-MM-DD") : "1970-01-01";
    var timestamp = new Date(time);
    return sqlFPConnection
        .sqlFP
        .query(`select k.isdeleted, k.code, k.createdutc, k.ProductionOrderOrderNo, k.grade, k.cartcartnumber, k.CartQty, ki.Id InstructionId, 
    ki.Code instructionCode, ki.name instructionname, ks.id stepid, ks.code stepcode, ks.process stepname, m.Code machineCode, m.Name machineName, m.MonthlyCapacity machineMonthlycapacity,
    ks.Deadline, k.CurrentStepIndex, ks.ProcessArea, k.IsComplete, k.ProductionOrderSalesContractNo, k.ProductionOrderProcessTypeName, k.ProductionOrderOrderTypeName, k.IsBadOutput, k.IsReprocess,
    k.OldKanbanId, k.Id, ks.stepindex
    from kanbans k left join kanbaninstructions ki on k.id = ki.kanbanid left join kanbansteps ks on ki.id = ks.instructionid left join machine m on ks.machineid = m.id where k.code = '084ZM6N5'
    where k.lastmodifiedutc >= :tanggal`, {
            replacements: { tanggal: timestamp },
            type: sqlFPConnection.sqlFP.QueryTypes.SELECT
        });
}

function transform(data) {
    data.forEach(element => {
        element.isdeleted = `'${element.isdeleted}'`;
        element.Deadline = element.Deadline ? `'${moment(element.Deadline).add(7, "hours").format("YYYY-MM-DD")}'` : null;
        element.createdutc = element.createdutc ? `'${moment(element.createdutc).add(7, "hours").format("YYYY-MM-DD")}'` : null;
        element.stepsLength = null;
        element.code = element.code ? `'${element.code}'` : null;
        element.grade = element.grade ? `'${element.grade}'` : null;
        element.cartcartnumber = element.cartcartnumber ? `'${element.cartcartnumber}'` : null;
        element.ProductionOrderOrderNo = element.ProductionOrderOrderNo ? `'${element.ProductionOrderOrderNo}'` : null;
        element.CartQty = element.CartQty != undefined ? `${element.CartQty}` : null;
        element.InstructionId = element.InstructionId ? `'${element.InstructionId}'` : null;
        element.instructionCode = element.instructionCode ? `'${element.instructionCode}'` : null;
        element.instructionname = element.instructionname ? `'${element.instructionname}'` : null;
        element.stepid = element.stepid ? `'${element.stepid}'` : null;
        element.stepcode = element.stepcode ? `'${element.stepcode}'` : null;
        element.stepname = element.stepname ? `'${element.stepname}'` : null;
        element.machineName = element.machineName ? `'${element.machineName}'` : null;
        element.machineCode = element.machineCode ? `'${element.machineCode}'` : null;
        element.machineMonthlycapacity = element.machineMonthlycapacity != undefined ? `${element.machineMonthlycapacity}` : null;
        element.ProcessArea = element.ProcessArea ? `'${element.ProcessArea}'` : null;
        element.IsComplete = `'${element.IsComplete}'`
        element.ProductionOrderSalesContractNo = element.ProductionOrderSalesContractNo ? `'${element.ProductionOrderSalesContractNo}'` : null;
        element.ProductionOrderProcessTypeName = element.ProductionOrderProcessTypeName ? `'${element.ProductionOrderProcessTypeName}'` : null;
        element.ProductionOrderOrderTypeName = element.ProductionOrderOrderTypeName ? `'${element.ProductionOrderOrderTypeName}'` : null;
        element.IsBadOutput = element.IsBadOutput != undefined ? `'${element.IsBadOutput}'` : null;
        element.IsReprocess = element.IsReprocess != undefined ? `'${element.IsReprocess}'` : null;
        element.OldKanbanId = element.OldKanbanId != undefined ? `'${element.OldKanbanId}'` : null;
        element.Id = `'${element.Id}'`;
    });

    return Promise.resolve(data);
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
                var sqlQuery = 'INSERT INTO [DL_Fact_Kanban_Temp] ';

                var count = 1;
                for (var item of data) {
                    if (item) {
                        var queryString = `\nSELECT ${item.isdeleted}, ${item.code}, ${item.createdutc}, ${item.ProductionOrderOrderNo}, ${item.grade}, ${item.cartcartnumber}, ${item.CartQty}, ${item.InstructionId}, ${item.instructionCode}, ${item.instructionname}, ${item.stepid}, ${item.stepcode}, ${item.stepname}, ${item.machineCode}, ${item.machineName}, ${item.machineMonthlycapacity}, ${item.Deadline}, ${item.CurrentStepIndex}, ${item.ProcessArea}, ${item.IsComplete}, ${item.stepsLength}, ${item.stepindex}, ${item.ProductionOrderSalesContractNo}, ${item.ProductionOrderProcessTypeName}, ${item.ProductionOrderOrderTypeName}, ${item.IsBadOutput}, ${item.IsReprocess}, ${item.OldKanbanId}, ${item.Id} UNION ALL `;
                        sqlQuery = sqlQuery.concat(queryString);
                        if (count % 1000 === 0) {
                            sqlQuery = sqlQuery.substring(0, sqlQuery.length - 10);
                            command.push(insertQuery(sqlDWHConnections.sqlDWH, sqlQuery, t));
                            sqlQuery = "INSERT INTO [DL_Fact_Kanban_Temp] ";
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
                        sqlDWHConnections.sqlDWH.query("exec DL_UPSERT_FACT_KANBAN", {
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
