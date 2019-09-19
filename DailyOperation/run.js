let sqlDWHConnections = require('../Connection/DWH/');
let sqlFPConnection = require('../Connection/FinishingPrinting/')
const MIGRATION_LOG_DESCRIPTION = "Fact Daily Operation from MongoDB to Azure DWH";
let moment = require('moment');

module.exports = async function () {
    var startedDate = new Date();
    return await timestamp()
        .then((times) => joinDailyOperation(times))
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

const extractDailyOperation = async function (times) {
    var time = times.length > 0 ? moment(times[0].start).format("YYYY-MM-DD") : "1970-01-01";
    var timestamp = new Date(time);
    return sqlFPConnection
        .sqlFP
        .query(`select
        d.IsDeleted _deleted,
        d.badOutput,
        db.Description badOutputDescription,
        d.code,
        d.dateInput,
        d.dateOutput,
        d.goodOutput,
        d.input,
        d.shift,
        d.timeInput,
        d.timeOutput,
        d.kanbanCode,
        k.Grade kanbanGrade,
        k.CartCartNumber kanbanCartCartNumber,
        k.CartCode kanbanCartCode,
        k.CartPcs kanbanCartPcs,
        k.CartQty kanbanCartQty,
        ki.Code kanbanInstructionCode,
        ki.Name kanbanInstructionName,
        k.ProductionOrderOrderTypeName orderType,
        k.SelectedProductionOrderDetailColorRequest,
        k.SelectedProductionOrderDetailColorTemplate,
        m.Code machineCode,
        m.Condition machineCondition,
        m.Manufacture machineManufacture,
        m.MonthlyCapacity machineMonthlyCapacity,
        m.Name machineName,
        m.Process machineProcess,
        m.Year machineYear,
        k.selectedProductionOrderDetailUomUnit,
        d.type,
        d.StepId stepProcessId,
        d.StepProcess,
        k.ProductionOrderOrderNo productionOrderNo,
        k.ProductionOrderSalesContractNo salesContractNo,
        db.action 
        from DailyOperation d left join dailyoperationbadoutputreasons db on d.id = db.DailyOperationId left join Kanbans k on d.KanbanId = k.Id left join KanbanInstructions ki on k.Id = ki.KanbanId left join Machine m on d.MachineId = m.Id        
        where d.lastmodifiedutc > ?
        order by d.code, d.type`, {
            replacements: [timestamp],
            type: sqlFPConnection.sqlFP.QueryTypes.SELECT
        });

};

const extractDailyOperationBadOutputReason = async function (times) {
    var time = times.length > 0 ? moment(times[0].start).format("YYYY-MM-DD") : "1970-01-01";
    var timestamp = new Date(time);
    return sqlFPConnection
        .sqlFP
        .query(`select
        d.code,
        db.badOutputCode,
        db.badOutputReason,
        db.length,
        db.description,
        db.action,
        db.id,
        db.dailyOperationId
        from DailyOperationBadOutputReasons db  inner join DailyOperation d  on d.Id = db.DailyOperationId
        where d.lastmodifiedutc > ?
        `, {
            replacements: [timestamp],
            type: sqlFPConnection.sqlFP.QueryTypes.SELECT
        });

};

const joinDailyOperation = function (times) {
    var dailyOperation = extractDailyOperation(times);
    var dailyOperationBadOutputReason = extractDailyOperationBadOutputReason(times);

    return Promise.all([dailyOperation, dailyOperationBadOutputReason])
        .then((data) => {
            var dailyOperation = data[0];
            var dailyOperationBadOutputReason = data[1];

            var results = {};
            results.dailyOperation = dailyOperation;
            results.dailyOperationBadOutputReason = dailyOperationBadOutputReason;

            return Promise.resolve(results);
        });
};

function transform(data) {
    var result = data.dailyOperation.map((item) => {

        return {
            _deleted: `'${item._deleted}'`,
            badOutput: item.badOutput ? `${item.badOutput}` : null,
            badOutputDescription: item.badOutputDescription ? `'${item.badOutputDescription}'` : null,
            code: item.code ? `'${item.code}'` : null,
            inputDate: item.dateInput ? `'${moment(item.dateInput).add(7, "hours").format("YYYY-MM-DD")}'` : null,
            outputDate: item.dateOutput ? `'${moment(item.dateOutput).add(7, "hours").format("YYYY-MM-DD")}'` : null,
            goodOutput: item.goodOutput ? `'${item.goodOutput}'` : null,
            input: item.input ? `${item.input}` : null,
            shift: item.shift ? `'${item.shift}'` : null,
            inputTime: item.timeInput ? `'${moment(item.timeInput).add(7, "hours").format("HH:mm:ss")}'` : null,
            outputTime: item.timeOutput ? `'${moment(item.timeOutput).add(7, "hours").format("HH:mm:ss")}'` : null,
            kanbanCode: item.kanbanCode ? `'${item.kanbanCode}'` : null,
            kanbanGrade: item.kanbanGrade ? `'${item.kanbanGrade}'` : null,
            kanbanCartCartNumber: item.kanbanCartCartNumber ? `'${item.kanbanCartCartNumber}'` : null,
            kanbanCartCode: item.kanbanCartCode ? `'${item.kanbanCartCode}'` : null,
            kanbanCartPcs: item.kanbanCartPcs ? `${item.kanbanCartPcs}` : 0,
            kanbanCartQty: item.kanbanCartQty ? `${item.kanbanCartQty}` : 0,
            kanbanInstructionCode: item.kanbanInstructionCode ? `'${item.kanbanInstructionCode}'` : null,
            kanbanInstructionName: item.kanbanInstructionName ? `'${item.kanbanInstructionName}'` : null,
            orderType: item.orderType ? `'${item.orderType}'` : null,
            selectedProductionOrderDetailCode: null,
            selectedProductionOrderDetailColorRequest: item.SelectedProductionOrderDetailColorRequest ? `'${item.SelectedProductionOrderDetailColorRequest.replace(/'/g, '"')}'` : null,
            selectedProductionOrderDetailColorTemplate: item.SelectedProductionOrderDetailColorTemplate ? `'${item.SelectedProductionOrderDetailColorTemplate.replace(/'/g, '"')}'` : null,
            machineCode: item.machineCode ? `'${item.machineCode}'` : null,
            machineCondition: item.machineCondition ? `'${item.machineCondition}'` : null,
            machineManufacture: item.machineManufacture ? `'${item.machineManufacture}'` : null,
            machineMonthlyCapacity: item.machineMonthlyCapacity ? `${item.machineMonthlyCapacity}` : null,
            machineName: item.machineName ? `'${item.machineName}'` : null,
            machineProcess: item.machineProcess ? `'${item.machineProcess}'` : null,
            machineYear: item.machineYear ? `'${item.machineYear}'` : null,
            inputQuantityConvertion: item.selectedProductionOrderDetailUomUnit && item.input ? `${item.input}` : null,
            goodOutputQuantityConvertion: item.goodOutput && item.selectedProductionOrderDetailUomUnit ? `${item.goodOutput}` : null,
            badOutputQuantityConvertion: item.badOutput && item.selectedProductionOrderDetailUomUnit ? `${item.badOutput}` : null,
            failedOutputQuantityConvertion: item.failedOutput && item.selectedProductionOrderDetailUomUnit ? `${item.failedOutput}` : null,
            outputQuantity: null,
            inputOutputDiff: null,
            status: null,
            type: item.type ? `'${item.type}'` : null,
            stepProcessId: item.stepProcessId ? `'${item.stepProcessId}'` : null,
            stepProcess: item.StepProcess ? `'${item.StepProcess}'` : null,
            processArea: null,
            productionOrderNo: item.productionOrderNo ? `'${item.productionOrderNo}'` : null,
            salesContractNo: item.salesContractNo ? `'${item.salesContractNo}'` : null,
            action: item.action ? `'${item.action.replace(/'/g, '"')}'` : null
        }
    });

    var badOutputReasons = data.dailyOperationBadOutputReason.map((reasonObj) => {
        return {
            dailyOperationCode: `'${reasonObj.code}'`,
            badOutputReasonCode: reasonObj.badOutputCode ? `'${reasonObj.badOutputCode}'` : null,
            reason: reasonObj.badOutputReason ? `'${reasonObj.badOutputReason.replace(/'/g, '"')}'` : null,
            length: reasonObj.length ? `${reasonObj.length}` : 0,
            description: reasonObj.description ? `'${reasonObj.description.replace(/'/g, '"')}'` : null,
            action: reasonObj.action ? `'${reasonObj.action.replace(/'/g, '"')}'` : null
        };
    })

    badOutputReasons = badOutputReasons.filter(function (element) {
        return element !== undefined;
    });

    var dailyOperationData = {
        results: [].concat.apply([], result),
        badOutputReasons: [].concat.apply([], badOutputReasons)
    };

    return Promise.resolve(dailyOperationData);
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
                var sqlQuery = 'INSERT INTO [DL_FACT_DAILY_OPERATION_TEMP] ';

                var count = 1;
                for (var item of data.results) {
                    if (item) {
                        var queryString = `\nSELECT ${item._deleted}, ${item.badOutput}, ${item.badOutputDescription}, ${item.code}, ${item.inputDate}, ${item.outputDate}, ${item.goodOutput}, ${item.input}, ${item.shift}, ${item.inputTime}, ${item.outputTime}, ${item.kanbanCode}, ${item.kanbanGrade}, ${item.kanbanCartCartNumber}, ${item.kanbanCartCode}, ${item.kanbanCartPcs}, ${item.kanbanCartQty}, ${item.kanbanInstructionCode}, ${item.kanbanInstructionName}, ${item.orderType}, ${item.selectedProductionOrderDetailCode}, ${item.selectedProductionOrderDetailColorRequest}, ${item.selectedProductionOrderDetailColorTemplate}, ${item.machineCode}, ${item.machineCondition}, ${item.machineManufacture}, ${item.machineMonthlyCapacity}, ${item.machineName}, ${item.machineProcess}, ${item.machineYear}, ${item.inputQuantityConvertion}, ${item.goodOutputQuantityConvertion}, ${item.badOutputQuantityConvertion}, ${item.failedOutputQuantityConvertion}, ${item.outputQuantity}, ${item.inputOutputDiff}, ${item.status}, ${item.type}, ${item.stepProcessId}, ${item.stepProcess}, ${item.processArea}, ${item.productionOrderNo}, ${item.salesContractNo}, ${item.action} UNION ALL `;
                        sqlQuery = sqlQuery.concat(queryString);
                        if (count % 500 == 0) {
                            sqlQuery = sqlQuery.substring(0, sqlQuery.length - 10);
                            command.push(insertQuery(sqlDWHConnections.sqlDWH, sqlQuery, t));
                            sqlQuery = "INSERT INTO [DL_FACT_DAILY_OPERATION_TEMP] ";
                        }
                        console.log(`add data to query  : ${count}`);
                        count++;
                    }
                }


                if (sqlQuery != "") {
                    sqlQuery = sqlQuery.substring(0, sqlQuery.length - 10);
                    command.push(insertQuery(sqlDWHConnections.sqlDWH, `${sqlQuery}`, t));
                }

                if (data.badOutputReasons && data.badOutputReasons.length > 0) {
                    var sqlQueryReason = 'INSERT INTO [DL_Fact_Daily_Operation_Reason_Temp](dailyOperationCode, badOutputReasonCode, reason, length, description, action) ';

                    var countReason = 1;

                    for (var item of data.badOutputReasons) {
                        if (item) {
                            var queryString = `\nSELECT ${item.dailyOperationCode}, ${item.badOutputReasonCode}, ${item.reason}, ${item.length}, ${item.description}, ${item.action} UNION ALL `;
                            sqlQueryReason = sqlQueryReason.concat(queryString);
                            if (countReason % 1000 === 0) {
                                sqlQueryReason = sqlQueryReason.substring(0, sqlQueryReason.length - 10);
                                command.push(insertQuery(sqlDWHConnections.sqlDWH, sqlQueryReason, t));
                                sqlQueryReason = "INSERT INTO [DL_Fact_Daily_Operation_Reason_Temp](dailyOperationCode, badOutputReasonCode, reason, length, description, action) ";
                            }
                            console.log(`add data to query  : ${countReason}`);
                            countReason++;
                        }
                    }

                    if (sqlQueryReason != "") {
                        sqlQueryReason = sqlQueryReason.substring(0, sqlQueryReason.length - 10);
                        command.push(insertQuery(sqlDWHConnections.sqlDWH, `${sqlQueryReason}`, t));
                    }
                }


                return Promise.all(command)
                    .then((results) => {
                        sqlDWHConnections.sqlDWH.query("exec [DL_UPSERT_FACT_DAILY_OPERATION]", {
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
                            console.log("rollback")
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
                        console.log("rollback")
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