let sqlDWHConnections = require('../Connection/DWH/');
let sqlFPConnection = require('../Connection/FinishingPrinting/')
const MIGRATION_LOG_DESCRIPTION = "Fabric QC from MongoDB to Azure DWH";
let moment = require('moment');
var dataCount = 0;


module.exports = async function () {
    var startedDate = new Date();
    return await timestamp()
        .then((times) => extractFQC(times))
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
                status: "Successful-30-Part23-" + dataCount
            };
            // return updateLog;
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

const extractFQC = async function (times) {
    var time = times.length > 0 ? moment(times[0].start).format("YYYY-MM-DD") : "1970-01-01";
    var timestamp = new Date(time);
    var fabricQC = await sqlFPConnection
        .sqlFP
        .query(`select id, code, pointSystem, dateIm, shiftIm, [group], operatorIm, MachineNoIm, 
        ProductionOrderNo, productionOrderType, kanbanCode, cartNo, Buyer, orderQuantity, 
        color, construction, packingInstruction, uom, IsDeleted, IsUsed from fabricqualitycontrols
        where lastmodifiedutc >= ?
        order by id
        offset 600 rows
        fetch next 30 rows only`, {
            replacements: [timestamp],
            type: sqlFPConnection.sqlFP.QueryTypes.SELECT
        });

    for (var element of fabricQC) {
        element.fabricGradeTests = await joinFGT(element);
    }

    return fabricQC;
};

const joinFGT = async function (data) {

    var fabricGT = await sqlFPConnection
        .sqlFP
        .query(`select id, type, pcsNo, grade, width, initLength, avalLength, finalLength, sampleLength, fabricGradeTest, finalGradeTest, score, finalScore, pointSystem, pointLimit from FabricGradeTests where FabricQualityControlId = ?`, {
            replacements: [data.id],
            type: sqlFPConnection.sqlFP.QueryTypes.SELECT
        });

    for (var element of fabricGT) {
        element.criteria = await joinCriterion(element);
    }

    return fabricGT;
};

const joinCriterion = async function (data) {
    var criteria = await sqlFPConnection
        .sqlFP
        .query(`select id, code, [Group], name, scoreA, scoreB, scoreC, scoreD from criterion where fabricgradetestid = ?`, {
            replacements: [data.id],
            type: sqlFPConnection.sqlFP.QueryTypes.SELECT
        });

    return criteria;
};

const transform = function (data) {
    var result = data.map((qualityControl) => {
        var results = qualityControl.fabricGradeTests.map((gradeTest) => {
            var resultss = gradeTest.criteria.map((criteria) => {
                var scoreA = criteria.scoreA >= 0 && gradeTest.pointSystem === 10 ? criteria.scoreA * 1 : null;
                var scoreB = criteria.scoreB >= 0 && gradeTest.pointSystem === 10 ? criteria.scoreB * 3 : null;
                var scoreC = criteria.scoreC >= 0 && gradeTest.pointSystem === 10 ? criteria.scoreC * 5 : null;
                var scoreD = criteria.scoreD >= 0 && gradeTest.pointSystem === 10 ? criteria.scoreD * 10 : null;
                var totalScore = scoreA + scoreB + scoreC + scoreD;
                return {
                    qcCode: qualityControl.code && qualityControl.code !== '' ? `'${qualityControl.code.replace(/'/g, '"')}'` : null,
                    qcpointSystem: qualityControl.pointSystem >= 0 && qualityControl.pointSystem !== '' && qualityControl.pointSystem ? `'${qualityControl.pointSystem}'` : null,
                    dateIm: qualityControl.dateIm ? `'${moment(qualityControl.dateIm).add(7, "hours").format("YYYY-MM-DD")}'` : null,
                    shiftIm: qualityControl.shiftIm && qualityControl.shiftIm !== '' ? `'${qualityControl.shiftIm.replace(/'/g, '"')}'` : null,
                    group: qualityControl.group && qualityControl.group !== '' ? `'${qualityControl.group.replace(/'/g, '"')}'` : null,
                    operatorIm: qualityControl.operatorIm && qualityControl.operatorIm !== '' ? `'${qualityControl.operatorIm.replace(/'/g, '"')}'` : null,
                    machineNoIm: qualityControl.MachineNoIm && qualityControl.MachineNoIm !== '' ? `'${qualityControl.MachineNoIm.replace(/'/g, '"')}'` : null,
                    productionOrderNo: qualityControl.ProductionOrderNo && qualityControl.ProductionOrderNo !== '' ? `'${qualityControl.ProductionOrderNo.replace(/'/g, '"')}'` : null,
                    productionOrderType: qualityControl.productionOrderType && qualityControl.productionOrderType !== '' ? `'${qualityControl.productionOrderType.replace(/'/g, '"')}'` : null,
                    kanbanCode: qualityControl.kanbanCode && qualityControl.kanbanCode !== '' ? `'${qualityControl.kanbanCode.replace(/'/g, '"')}'` : null,
                    cartNo: qualityControl.cartNo && qualityControl.cartNo !== '' ? `'${qualityControl.cartNo.replace(/'/g, '"')}'` : null,
                    buyer: qualityControl.Buyer && qualityControl.Buyer !== '' ? `'${qualityControl.Buyer.replace(/'/g, '"')}'` : null,
                    orderQuantity: qualityControl.orderQuantity >= 0 && qualityControl.orderQuantity !== '' && qualityControl.orderQuantity ? `'${qualityControl.orderQuantity}'` : null,
                    color: qualityControl.color && qualityControl.color !== '' ? `'${qualityControl.color.replace(/'/g, '"')}'` : null,
                    construction: qualityControl.construction && qualityControl.construction !== '' ? `'${qualityControl.construction.replace(/'/g, '"')}'` : null,
                    packingInstruction: qualityControl.packingInstruction && qualityControl.packingInstruction !== '' ? `'${qualityControl.packingInstruction.replace(/'/g, '"')}'` : null,
                    uom: qualityControl.uom && qualityControl.uom !== '' ? `'${qualityControl.uom.replace(/'/g, '"')}'` : null,
                    type: gradeTest.type && gradeTest.type !== '' ? `'${gradeTest.type.replace(/'/g, '"')}'` : null,
                    pcsNo: gradeTest.pcsNo && gradeTest.pcsNo !== '' ? `'${gradeTest.pcsNo.replace(/'/g, '"')}'` : null,
                    grade: gradeTest.grade && gradeTest.grade !== '' ? `'${gradeTest.grade.replace(/'/g, '"')}'` : null,
                    width: gradeTest.width >= 0 && gradeTest.width !== '' && gradeTest.width != null ? `'${gradeTest.width}'` : null,
                    initLength: gradeTest.initLength >= 0 && gradeTest.initLength && gradeTest.initLength != null ? `'${gradeTest.initLength}'` : null,
                    avalLength: gradeTest.avalLength >= 0 && gradeTest.avalLength !== '' && gradeTest.avalLength != null ? `'${gradeTest.avalLength}'` : null,
                    finalLength: gradeTest.finalLength >= 0 && gradeTest.finalLength !== '' && gradeTest.finalLength != null ? `'${gradeTest.finalLength}'` : null,
                    sampleLength: gradeTest.sampleLength >= 0 && gradeTest.sampleLength !== '' && gradeTest.sampleLength != null ? `'${gradeTest.sampleLength}'` : null,
                    fabricGradeTest: gradeTest.fabricGradeTest >= 0 && gradeTest.fabricGradeTest !== '' && gradeTest.fabricGradeTest != null ? `'${gradeTest.fabricGradeTest}'` : null,
                    finalGradeTest: gradeTest.finalGradeTest >= 0 && gradeTest.finalGradeTest !== '' && gradeTest.finalGradeTest != null ? `'${gradeTest.finalGradeTest}'` : null,
                    score: gradeTest.score >= 0 && gradeTest.score !== '' && gradeTest.score != null ? `'${gradeTest.score}'` : null,
                    finalScore: gradeTest.finalScore >= 0 && gradeTest.finalScore !== '' && gradeTest.finalScore != null ? `'${gradeTest.finalScore}'` : null,
                    pointSystem: gradeTest.pointSystem >= 0 && gradeTest.pointSystem !== '' && gradeTest.pointSystem != null ? `'${gradeTest.pointSystem}'` : null,
                    criteriaCode: criteria.code && criteria.code !== '' && criteria.code ? `'${criteria.code.replace(/'/g, '"')}'` : null,
                    criteriaGroup: criteria.Group && criteria.Group !== '' && criteria.Group ? `'${criteria.Group.replace(/'/g, '"')}'` : null,
                    criteriaName: criteria.name && criteria.name !== '' && criteria.name ? `'${criteria.name.replace(/'/g, '"')}'` : null,
                    criteriaA: criteria.scoreA >= 0 && criteria.scoreA !== '' && criteria.scoreA != null ? `${criteria.scoreA}` : null,
                    criteriaB: criteria.scoreB >= 0 && criteria.scoreB !== '' && criteria.scoreB != null ? `${criteria.scoreB}` : null,
                    criteriaC: criteria.scoreC >= 0 && criteria.scoreC !== '' && criteria.scoreC != null ? `${criteria.scoreC}` : null,
                    criteriaD: criteria.scoreD >= 0 && criteria.scoreD !== '' && criteria.scoreD != null ? `${criteria.scoreD}` : null,
                    totalScore: `${totalScore}`,
                    deleted: `'${qualityControl.IsDeleted}'`,
                    isUsed: `'${qualityControl.IsUsed}'`,
                    pointLimit: gradeTest.pointLimit >= 0 && gradeTest.pointLimit != null ? `'${gradeTest.pointLimit}'` : null
                }
            });
            return [].concat.apply([], resultss);
        });
        return [].concat.apply([], results);
    });
    return Promise.resolve([].concat.apply([], result));
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
                var sqlQuery = 'INSERT INTO [DL_Fact_Fabric_Quality_Control_Temp] ';

                var count = 1;
                dataCount = data.length;
                for (var item of data) {
                    if (item) {
                        var queryString = `\nSELECT ${item.qcCode}, ${item.qcpointSystem}, ${item.dateIm}, ${item.shiftIm}, ${item.group}, ${item.operatorIm}, ${item.machineNoIm}, ${item.productionOrderNo}, ${item.productionOrderType}, ${item.kanbanCode}, ${item.cartNo}, ${item.buyer}, ${item.orderQuantity}, ${item.color}, ${item.construction}, ${item.packingInstruction}, ${item.uom}, ${item.type}, ${item.pcsNo}, ${item.grade}, ${item.width}, ${item.initLength}, ${item.avalLength}, ${item.finalLength}, ${item.sampleLength}, ${item.fabricGradeTest}, ${item.finalGradeTest}, ${item.score}, ${item.finalScore}, ${item.pointSystem}, ${item.criteriaCode}, ${item.criteriaGroup}, ${item.criteriaName}, ${item.criteriaA}, ${item.criteriaB}, ${item.criteriaC}, ${item.criteriaD}, ${item.totalScore}, ${item.deleted}, ${item.isUsed}, ${item.pointLimit} UNION ALL `;
                        sqlQuery = sqlQuery.concat(queryString);

                        if (count % 1000 === 0) {
                            sqlQuery = sqlQuery.substring(0, sqlQuery.length - 10);
                            command.push(insertQuery(sqlDWHConnections.sqlDWH, sqlQuery, t));
                            sqlQuery = "INSERT INTO [DL_Fact_Fabric_Quality_Control_Temp] ";
                        }
                        console.log(`add data to query  : ${count}`);
                        count++;
                    }
                }

                if (sqlQuery != "") {
                    sqlQuery = sqlQuery.substring(0, sqlQuery.length - 10);
                    command.push(insertQuery(sqlDWHConnections.sqlDWH, `${sqlQuery}`, t));
                }

                // return Promise.all(command)
                //     .then((execResult) => {
                //         t.commit()
                //             .then(() => {
                //                 resolve(execResult);
                //             })
                //             .catch((err) => {
                //                 reject(err);
                //             });


                //     }).catch((error) => {
                //         t.rollback()
                //             .then(() => {
                //                 reject(error);
                //             })
                //             .catch((err) => {
                //                 reject(err);
                //             });
                //     });

                return Promise.all(command)
                    .then((results) => {
                        sqlDWHConnections.sqlDWH.query("exec DL_UPSERT_FACT_FABRIC_QUALITY_CONTROL", {
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