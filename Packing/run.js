let sqlDWHConnections = require('../Connection/DWH/');
let sqlFPConnection = require('../Connection/FinishingPrinting/')
const MIGRATION_LOG_DESCRIPTION = "Fact Packing from MongoDB to Azure DWH";
let moment = require('moment');

module.exports = async function () {
    var startedDate = new Date();
    return await timestamp()
        .then((times) => extractPacking(times))
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

const extractPacking = async function (times) {
    var time = times.length > 0 ? moment(times[0].start).format("YYYY-MM-DD") : "1970-01-01";
    var timestamp = new Date(time);
    return sqlFPConnection
        .sqlFP
        .query(`select
        p.IsDeleted _deleted,
        p.CreatedBy _createdBy,
        p.CreatedUtc _createdDate,
        p.code,
        p.productionOrderId,
        p.productionOrderNo,
        p.OrderTypeName orderType,
        p.salesContractNo,
        p.designCode,
        p.designNumber,
        p.buyerId,
        p.buyerCode,
        p.buyerName,
        p.buyerAddress,
        p.buyerType,
        p.date,
        p.packingUom,
        p.colorCode,
        p.colorName,
        p.colorType,
        p.materialConstructionFinishId,
        p.materialConstructionFinishName,
        p.materialId,
        p.material,
        p.materialWidthFinish,
        p.construction,
        p.deliveryType,
        p.finishedProductType,
        p.motif,
        pd.lot,
        pd.grade,
        pd.weight,
        pd.length,
        pd.quantity,
        pd.remark,
        p.status,
        p.accepted,
        p.declined
        from packings p left join packingdetails pd on p.Id = pd.PackingId
        where p.lastmodifiedutc >= ?
        `, {
            replacements: [timestamp],
            type: sqlFPConnection.sqlFP.QueryTypes.SELECT
        });

};

function transform(data) {

    var packingData = data.map((packing) => {
        return {
            deleted: `'${packing._deleted}'`,
            createdBy: packing._createdBy ? `'${packing._createdBy}'` : null,
            createdDate: packing._createdDate ? `'${moment(packing._createdDate).add(7, 'hours').format('YYYY-MM-DD')}'` : null,
            code: packing.code ? `'${packing.code}'` : null,
            productionOrderId: packing.productionOrderId ? `'${packing.productionOrderId}'` : null,
            productionOrderNo: packing.productionOrderNo ? `'${packing.productionOrderNo}'` : null,
            orderType: packing.orderType ? `'${packing.orderType}'` : null,
            salesContractNo: packing.salesContractNo ? `'${packing.salesContractNo}'` : null,
            designCode: packing.designCode ? `'${packing.designCode.replace(/'/g, '"')}'` : null,
            designNumber: packing.designNumber ? `'${packing.designNumber.replace(/'/g, '"')}'` : null,
            buyerId: packing.buyerId ? `'${packing.buyerId}'` : null,
            buyerCode: packing.buyerCode ? `'${packing.buyerCode}'` : null,
            buyerName: packing.buyerName ? `'${packing.buyerName.replace(/'/g, '"')}'` : null,
            buyerAddress: packing.buyerAddress ? `'${packing.buyerAddress.replace(/'/g, '"')}'` : null,
            buyerType: packing.buyerType ? `'${packing.buyerType}'` : null,
            date: packing.date ? `'${moment(packing.date).add(7, 'hours').format('YYYY-MM-DD')}'` : null,
            packingUom: packing.packingUom ? `'${packing.packingUom}'` : null,
            colorCode: packing.colorCode ? `'${packing.colorCode.replace(/'/g, '"')}'` : null,
            colorName: packing.colorName ? `'${packing.colorName.replace(/'/g, '"')}'` : null,
            colorType: packing.colorType ? `'${packing.colorType.replace(/'/g, '"')}'` : null,
            materialConstructionFinishId: packing.materialConstructionFinishId ? `'${packing.materialConstructionFinishId}'` : null,
            materialConstructionFinishName: packing.materialConstructionFinishName ? `'${packing.materialConstructionFinishName.replace(/'/g, '"')}'` : null,
            materialId: packing.materialId ? `'${packing.materialId}'` : null,
            material: packing.material ? `'${packing.material.replace(/'/g, '"')}'` : null,
            materialWidthFinish: packing.materialWidthFinish ? `'${packing.materialWidthFinish.replace(/'/g, '"')}'` : null,
            construction: packing.construction ? `'${packing.construction.replace(/'/g, '"')}'` : null,
            deliveryType: packing.deliveryType ? `'${packing.deliveryType.replace(/'/g, '"')}'` : null,
            finishedProductType: packing.finishedProductType ? `'${packing.finishedProductType.replace(/'/g, '"')}'` : null,
            motif: packing.motif ? `'${packing.motif.replace(/'/g, '"')}'` : null,
            lot: packing.lot ? `'${packing.lot.replace(/'/g, '"')}'` : null,
            grade: packing.grade ? `'${packing.grade.replace(/'/g, '"')}'` : null,
            weight: packing.weight ? `${packing.weight}` : 0,
            length: packing.length ? `${packing.length}` : 0,
            quantity: packing.quantity ? `${packing.quantity}` : 0,
            remark: packing.remark ? `'${packing.remark.replace(/'/g, '"')}'` : null,
            status: packing.status ? `'${packing.status.replace(/'/g, '"')}'` : null,
            accepted: `'${packing.accepted}'`,
            declined: `'${packing.declined}'`
        }
    });

    return Promise.resolve([].concat.apply([], packingData));
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
                var sqlQuery = 'INSERT INTO [DL_Fact_Packing_Temp] ';

                var count = 1;
                for (var item of data) {
                    if (item) {
                        var queryString = `\nSELECT ${item.deleted}, ${item.createdBy}, ${item.createdDate}, ${item.code}, ${item.productionOrderId}, ${item.productionOrderNo}, ${item.orderType}, ${item.salesContractNo}, ${item.designCode}, ${item.designNumber}, ${item.buyerId}, ${item.buyerCode}, ${item.buyerName}, ${item.buyerAddress}, ${item.buyerType}, ${item.date}, ${item.packingUom}, ${item.colorCode}, ${item.colorName}, ${item.colorType}, ${item.materialConstructionFinishId}, ${item.materialConstructionFinishName}, ${item.materialId}, ${item.material}, ${item.materialWidthFinish}, ${item.construction}, ${item.deliveryType}, ${item.finishedProductType}, ${item.motif}, ${item.lot}, ${item.grade}, ${item.weight}, ${item.length}, ${item.quantity}, ${item.remark}, ${item.status}, ${item.accepted}, ${item.declined} UNION ALL `;
                        sqlQuery = sqlQuery.concat(queryString);
                        if (count % 1000 == 0) {
                            sqlQuery = sqlQuery.substring(0, sqlQuery.length - 10);
                            command.push(insertQuery(sqlDWHConnections.sqlDWH, sqlQuery, t));
                            sqlQuery = "INSERT INTO [DL_Fact_Packing_Temp] ";
                        }
                        console.log(`add data to query  : ${count}`);
                        count++;
                    }
                }


                if (sqlQuery != "" && sqlQuery !== "INSERT INTO [DL_Fact_Packing_Temp] ") {
                    sqlQuery = sqlQuery.substring(0, sqlQuery.length - 10);
                    command.push(insertQuery(sqlDWHConnections.sqlDWH, `${sqlQuery}`, t));
                }


                return Promise.all(command)
                    .then((results) => {
                        sqlDWHConnections.sqlDWH.query("exec [DL_UPSERT_FACT_PACKING]", {
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