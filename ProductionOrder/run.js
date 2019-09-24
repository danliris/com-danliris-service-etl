let sqlDWHConnections = require('../Connection/DWH/');
let sqlSalesConnections = require('../Connection/Sales/')
const MIGRATION_LOG_DESCRIPTION = "Fact Production Order from MongoDB to Azure DWH";
let moment = require('moment');

module.exports = async function () {
    var startedDate = new Date();
    return await timestamp()
        .then((times) => extractPO(times))
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

const extractPO = async function (times) {
    var time = times.length > 0 ? moment(times[0].start).format("YYYY-MM-DD") : "1970-01-01";
    var timestamp = new Date(time);
    return await sqlSalesConnections
        .sqlSales
        .query(`SELECT 
        salesContractNo, 
        OrderNo, 
        OrderTypeName,
        ProcessTypeName, 
        MaterialName,
        MaterialConstructionName,
        YarnMaterialName, 
        materialWidth, 
        orderQuantity, 
        UomUnit, 
        BuyerName, 
        BuyerType,
        deliveryDate,
        CreatedUtc _createdDate,
        BuyerCode, 
        IsDeleted  _deleted, 
        AccountUserName, 
        isClosed,
        orderQuantity
        FROM ProductionOrder
        where lastmodifiedutc >= ?
        `, {
            replacements: [timestamp],
            type: sqlSalesConnections.sqlSales.QueryTypes.SELECT
        });

};

function orderQuantityConvertion(uom, quantity) {
    if (uom.toLowerCase() === "met" || uom.toLowerCase() === "mtr" || uom.toLowerCase() === "pcs") {
        return quantity;
    } else if (uom.toLowerCase() === "yard" || uom.toLowerCase() === "yds") {
        return quantity * 0.9144;
    } else {
        return quantity;
    }
}

function joinConstructionString(material, materialConstruction, yarnMaterialNo, materialWidth) {
    if (material !== null && materialConstruction !== null && yarnMaterialNo !== null && materialWidth !== null) {
        return `'${material.replace(/'/g, '"') + " " + materialConstruction.replace(/'/g, '"') + " " + yarnMaterialNo.replace(/'/g, '"') + " " + materialWidth.replace(/'/g, '"')}'`;
    } else {
        return null;
    }
}

function transform(data) {
    var result = data.map((items) => {
        var item = items;
        // var kanban = items.kanban;
        var orderUom = item.UomUnit ? item.UomUnit : null;
        var orderQuantity = item.orderQuantity ? item.orderQuantity : null;
        var material = item.MaterialName ? item.MaterialName.replace(/'/g, '"') : null;
        var materialConstruction = item.MaterialConstructionName ? item.MaterialConstructionName.replace(/'/g, '"') : null;
        var yarnMaterialNo = item.YarnMaterialName ? item.YarnMaterialName.replace(/'/g, '"') : null;
        var materialWidth = item.materialWidth ? item.materialWidth : null;

        return {
            salesContractNo: item.salesContractNo ? `'${item.salesContractNo.replace(/'/g, '"')}'` : null,
            productionOrderNo: item.OrderNo ? `'${item.OrderNo.replace(/'/g, '"')}'` : null,
            orderType: item.OrderTypeName ? `'${item.OrderTypeName.replace(/'/g, '"')}'` : null,
            processType: item.ProcessTypeName ? `'${item.ProcessTypeName.replace(/'/g, '"')}'` : null,
            material: item.MaterialName ? `'${item.MaterialName.replace(/'/g, '"')}'` : null,
            materialConstruction: item.MaterialConstructionName ? `'${item.MaterialConstructionName.replace(/'/g, '"')}'` : null,
            yarnMaterialNo: item.YarnMaterialName ? `'${item.YarnMaterialName.replace(/'/g, '"')}'` : null,
            materialWidth: item.materialWidth ? `'${item.materialWidth.replace(/'/g, '"')}'` : null,
            orderQuantity: item.orderQuantity ? `${item.orderQuantity}` : null,
            orderUom: item.UomUnit ? `'${item.UomUnit.replace(/'/g, '"')}'` : null,
            buyer: item.BuyerName ? `'${item.BuyerName.replace(/'/g, '"')}'` : null,
            buyerType: item.BuyerType ? `'${item.BuyerType.replace(/'/g, '"')}'` : null,
            deliveryDate: item.deliveryDate ? `'${moment(item.deliveryDate).add(7, "hours").format("YYYY-MM-DD")}'` : null,
            createdDate: item._createdDate ? `'${moment(item._createdDate).add(7, "hours").format("YYYY-MM-DD")}'` : null,
            totalOrderConvertion: item.orderQuantity ? `${orderQuantityConvertion(orderUom, orderQuantity)}` : null,
            construction: joinConstructionString(material.replace(/'/g, '"'), materialConstruction.replace(/'/g, '"'), yarnMaterialNo.replace(/'/g, '"'), materialWidth),
            buyerCode: item.BuyerCode ? `'${item.BuyerCode.replace(/'/g, '"')}'` : null,
            cartQuantity: null,
            kanbanCode: null,
            deleted: `'${item._deleted}'`,
            username: item.AccountUserName ? `'${item.AccountUserName.replace(/'/g, '"')}'` : null,
            isClosed: `'${item.isClosed}'`,
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
                var sqlQuery = 'INSERT INTO [DL_Fact_Production_Order_Temp] ';
                var count = 1;
                for (var item of data) {
                    if (item) {
                        var queryString = `\nSELECT ${item.salesContractNo}, ${item.productionOrderNo}, ${item.orderType}, ${item.processType}, ${item.material}, ${item.materialConstruction}, ${item.yarnMaterialNo}, ${item.materialWidth}, ${item.orderQuantity}, ${item.orderUom}, ${item.buyer}, ${item.buyerType}, ${item.deliveryDate}, ${item.createdDate}, ${item.totalOrderConvertion}, ${item.construction}, ${item.buyerCode}, ${item.cartQuantity}, ${item.kanbanCode}, ${item.deleted}, ${item.username}, ${item.isClosed} UNION ALL `;
                        sqlQuery = sqlQuery.concat(queryString);

                        if (count % 1000 == 0) {
                            sqlQuery = sqlQuery.substring(0, sqlQuery.length - 10);
                            command.push(insertQuery(sqlDWHConnections.sqlDWH, sqlQuery, t));
                            sqlQuery = "INSERT INTO [DL_Fact_Production_Order_Temp] ";
                        }
                        console.log(`add data to query  : ${count}`);
                        count++;
                    }
                }


                if (sqlQuery != "" && sqlQuery !== "INSERT INTO [DL_Fact_Production_Order_Temp] ") {
                    sqlQuery = sqlQuery.substring(0, sqlQuery.length - 10);
                    command.push(insertQuery(sqlDWHConnections.sqlDWH, `${sqlQuery}`, t));
                }


                return Promise.all(command)
                    .then((results) => {
                        sqlDWHConnections.sqlDWH.query("exec [DL_UPSERT_FACT_PRODUCTION_ORDER]", {
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