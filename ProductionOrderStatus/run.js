let sqlDWHConnections = require('../Connection/DWH/');
let sqlFPConnection = require('../Connection/FinishingPrinting/');
let sqlSalesConnection = require('../Connection/Sales/');
const MIGRATION_LOG_DESCRIPTION = "Fact Production Order Status from MongoDB to Azure DWH";
let moment = require('moment');

module.exports = async function () {
    var startedDate = new Date();
    return await timestamp()
        .then((times) => extractFPSalesContract(times))
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

const extractFPSalesContract = async function (times) {
    var time = times.length > 0 ? moment(times[0].start).format("YYYY-MM-DD") : "1970-01-01";
    var timestamp = new Date(time);
    var fpSalesContracts = await sqlSalesConnection
        .sqlSales
        .query(`SELECT
        createdUtc,
        salesContractNo,
        uomUnit,
        orderQuantity,
        orderTypeName,
        isDeleted,
        deliverySchedule
        FROM FinishingPrintingSalesContracts
        where lastmodifiedutc >= :tanggal`, {
            replacements: { tanggal: timestamp },
            type: sqlSalesConnection.sqlSales.QueryTypes.SELECT
        });

    var results = [];
    for (var element of fpSalesContracts) {
        var item = {};
        item.finishingPrintingSalesContract = element;
        item.finishingPrintingSalesContract.uom = {};
        item.finishingPrintingSalesContract.uom.unit = item.finishingPrintingSalesContract.uomUnit;
        item.finishingPrintingSalesContract.orderType = {};
        item.finishingPrintingSalesContract.orderType.name = item.finishingPrintingSalesContract.orderTypeName;
        item.productionOrder = await joinProductionOrder(element);
        item.kanban = await joinKanban(item.productionOrder);
        item.dailyOperation = await joinDailyOperation(item.kanban);
        item.fabricQualityControl = await joinFabricQC(item.kanban);
        results.push(item);
    }

    return results;
};

const joinProductionOrder = async function (data) {

    var productionOrderList = await sqlSalesConnection
        .sqlSales
        .query(`SELECT
        id, 
        createdUtc,
        salesContractNo,
        orderNo,
        orderQuantity,
        uomUnit,
        deliveryDate
        FROM productionorder where salesContractNo = ? and isdeleted = 0`, {
            replacements: [data.salesContractNo],
            type: sqlSalesConnection.sqlSales.QueryTypes.SELECT
        });

    var productionOrder = productionOrderList[0];
    if (productionOrder) {
        productionOrder.uom = {};
        productionOrder.uom.unit = productionOrder.uomUnit;
    }

    return productionOrder;
};

const joinKanban = async function (data) {

    if (data) {
        var kanbanList = await sqlFPConnection
            .sqlFP
            .query(`SELECT
        id 
        createdUtc,
        code,
        productionOrderSalesContractNo,
        cartQty,
        productionOrderId,
        cartCartNumber,
        productionOrderOrderNo
        FROM Kanbans where productionorderid = ? and isdeleted = 0`, {
                replacements: [data.id],
                type: sqlFPConnection.sqlFP.QueryTypes.SELECT
            });

        var kanban = kanbanList[0];
        if (kanban) {
            kanban.productionOrder = {};
            kanban.productionOrder.salesContractNo = kanban.productionOrderSalesContractNo;
            kanban.productionOrder.orderNo = kanban.productionOrderOrderNo;
            kanban.productionOrder.uom = {};
            kanban.productionOrder.uom.unit = data.uom.unit;
            kanban.cart = {};
            kanban.cart.cartNumber = kanban.cartCartNumber;
            kanban.cart.qty = kanban.cartQty;
        }

        return kanban;
    } else {
        return null;
    }

};

const joinDailyOperation = async function (data) {

    if (data) {
        var dailyOperationList = await sqlFPConnection
            .sqlFP
            .query(`select 
        createdUtc,
        code,
        input,
        kanbanId
        from dailyoperation where kanbanId = ? and isdeleted = 0 and input is not null`, {
                replacements: [data.id],
                type: sqlFPConnection.sqlFP.QueryTypes.SELECT
            });

        var dailyOperation = dailyOperationList[0];

        if (dailyOperation) {
            dailyOperation.kanban = {};
            dailyOperation.kanban.productionOrder = {};
            dailyOperation.kanban.productionOrder.salesContractNo = data.productionOrder.salesContractNo;
        }

        return dailyOperation;
    } else {
        return null;
    }

};

const joinFabricQC = async function (data) {

    if (data) {
        var fabricQCList = await sqlFPConnection
            .sqlFP
            .query(`select
        id,
        dateIm,
        uom,
        code,
        kanbanCode
        from FabricQualityControls where kanbanCode = ? and isdeleted = 0`, {
                replacements: [data.code],
                type: sqlFPConnection.sqlFP.QueryTypes.SELECT
            });

        var fabricQC = fabricQCList[0];

        if (fabricQC) {
            fabricQC.fabricGradeTests = await joinFabricGradeTest(fabricQC);
        }

        return fabricQC;
    } else {
        return null;
    }

};

const joinFabricGradeTest = async function (data) {

    var fabricGradeTestList = await sqlFPConnection
        .sqlFP
        .query(`select
        initLength,
        fabricQualityControlId
        from FabricGradeTests where fabricQualityControlId = ?`, {
            replacements: [data.id],
            type: sqlFPConnection.sqlFP.QueryTypes.SELECT
        });

    var fabricGT = fabricGradeTestList[0];

    return fabricGT;
};

const orderQuantityConvertion = function (uom, quantity) {
    if (uom.toLowerCase() === "met" || uom.toLowerCase() === "mtr" || uom.toLowerCase() === "pcs") {
        return quantity;
    } else if (uom.toLowerCase() === "yard" || uom.toLowerCase() === "yds") {
        return quantity * 0.9144;
    } else {
        return quantity;
    }
}

const transform = function (data) {
    var result = data.map((item) => {
        var finishingPrintingSC = item.finishingPrintingSalesContract;
        var productionOrder = item.productionOrder;
        var kanban = item.kanban;
        var fabricQC = item.fabricQualityControl;
        var dailyOperation = item.dailyOperation;
        var index = 0;

        if (fabricQC) {
            var results = fabricQC.fabricGradeTests.map((fabricGradeTest) => {
                var quantity = fabricGradeTest.initLength;
                index++;

                return {
                    salesContractDate: finishingPrintingSC.createdUtc ? `'${moment(finishingPrintingSC.createdUtc).add(7, "hours").format("YYYY-MM-DD")}'` : null,
                    salesContractNo: finishingPrintingSC.salesContractNo ? `'${finishingPrintingSC.salesContractNo}'` : null,
                    salesContractQuantity: finishingPrintingSC.orderQuantity ? `${orderQuantityConvertion(finishingPrintingSC.uom.unit, finishingPrintingSC.orderQuantity)}` : null,
                    productionOrderDate: productionOrder.createdUtc ? `'${moment(productionOrder.createdUtc).add(7, "hours").format("YYYY-MM-DD")}'` : null,
                    productionSalesContractNo: productionOrder.salesContractNo ? `'${productionOrder.salesContractNo}'` : null,
                    productionOrderNo: productionOrder.orderNo ? `'${productionOrder.orderNo}'` : null,
                    productionOrderQuantity: productionOrder.orderQuantity ? `${orderQuantityConvertion(productionOrder.uom.unit, productionOrder.orderQuantity)}` : null,
                    kanbanDate: kanban.createdUtc ? `'${moment(kanban.createdUtc).add(7, "hours").format("YYYY-MM-DD")}'` : null,
                    kanbanCode: kanban.code ? `'${kanban.code}'` : null,
                    kanbanSalesContractNo: kanban.productionOrder && kanban.productionOrder.salesContractNo ? `'${kanban.productionOrder.salesContractNo}'` : null,
                    kanbanQuantity: kanban && kanban.cart.qty && kanban.productionOrder && kanban.productionOrder.uom && kanban.productionOrder.uom.unit ? `${orderQuantityConvertion(kanban.productionOrder.uom.unit, kanban.cart.qty)}` : null,
                    dailyOperationQuantity: dailyOperation ? `${dailyOperation.input}` : null,
                    dailyOperationSalesContractNo: dailyOperation ? `'${dailyOperation.kanban.productionOrder.salesContractNo}'` : null,
                    dailyOperationCode: dailyOperation ? `'${dailyOperation.code}'` : null,
                    cartNumber: kanban && kanban.cart.cartNumber ? `'${kanban.cart.cartNumber}'` : null,
                    fabricQualityControlDate: fabricQC.dateIm ? `'${moment(fabricQC.dateIm).add(7, "hours").format("YYYY-MM-DD")}'` : null,
                    fabricQualityControlQuantity: quantity ? `${quantity}` : null,
                    fabricQualityControlCode: fabricQC && fabricQC.code ? `'${fabricQC.code}'` : null,
                    orderType: finishingPrintingSC && finishingPrintingSC.orderType && finishingPrintingSC.orderType.name ? `'${finishingPrintingSC.orderType.name}'` : null,
                    deleted: `'${finishingPrintingSC.isDeleted}'`,
                    fabricqualitycontroltestindex: fabricQC.code ? `${index}` : null,
                    dailyOperationDate: dailyOperation && dailyOperation.createdUtc ? `'${moment(dailyOperation.createdUtc).add(7, "hours").format("YYYY-MM-DD")}'` : null,
                    salesContractDeliveryDate: finishingPrintingSC ? `'${moment(finishingPrintingSC.deliverySchedule).add(7, "hours").format("YYYY-MM-DD")}'` : null,
                    productionOrderDeliveryDate: productionOrder ? `'${moment(productionOrder.deliveryDate).add(7, "hours").format("YYYY-MM-DD")}'` : null
                }
            });
            return [].concat.apply([], results);
        } else {
            return {
                salesContractDate: finishingPrintingSC.createdUtc ? `'${moment(finishingPrintingSC.createdUtc).add(7, "hours").format("YYYY-MM-DD")}'` : null,
                salesContractNo: finishingPrintingSC.salesContractNo ? `'${finishingPrintingSC.salesContractNo}'` : null,
                salesContractQuantity: finishingPrintingSC.orderQuantity ? `${orderQuantityConvertion(finishingPrintingSC.uom.unit, finishingPrintingSC.orderQuantity)}` : null,
                productionOrderDate: productionOrder && productionOrder.createdUtc ? `'${moment(productionOrder.createdUtc).add(7, "hours").format("YYYY-MM-DD")}'` : null,
                productionSalesContractNo: productionOrder && productionOrder.salesContractNo ? `'${productionOrder.salesContractNo}'` : null,
                productionOrderNo: productionOrder && productionOrder.orderNo ? `'${productionOrder.orderNo}'` : null,
                productionOrderQuantity: productionOrder && productionOrder.orderQuantity ? `${orderQuantityConvertion(productionOrder.uom.unit, productionOrder.orderQuantity)}` : null,
                kanbanDate: kanban && kanban.createdUtc ? `'${moment(kanban.createdUtc).add(7, "hours").format("YYYY-MM-DD")}'` : null,
                kanbanCode: kanban && kanban.code ? `'${kanban.code}'` : null,
                kanbanSalesContractNo: kanban && kanban.productionOrder && kanban.productionOrder.salesContractNo ? `'${kanban.productionOrder.salesContractNo}'` : null,
                dailyOperationQuantity: dailyOperation ? `${dailyOperation.input}` : null,
                dailyOperationSalesContractNo: dailyOperation ? `'${dailyOperation.kanban.productionOrder.salesContractNo}'` : null,
                dailyOperationCode: dailyOperation ? `'${dailyOperation.code}'` : null,
                kanbanQuantity: kanban && kanban.cart.qty && kanban.productionOrder && kanban.productionOrder.uom && kanban.productionOrder.uom.unit ? `${orderQuantityConvertion(kanban.productionOrder.uom.unit, kanban.cart.qty)}` : null,
                cartNumber: kanban && kanban.cart.cartNumber ? `'${kanban.cart.cartNumber}'` : null,
                fabricQualityControlDate: null,
                fabricQualityControlQuantity: null,
                fabricQualityControlCode: null,
                orderType: finishingPrintingSC && finishingPrintingSC.orderType && finishingPrintingSC.orderType.name ? `'${finishingPrintingSC.orderType.name}'` : null,
                deleted: `'${finishingPrintingSC.isDeleted}'`,
                fabricqualitycontroltestindex: null,
                dailyOperationDate: dailyOperation && dailyOperation.createdUtc ? `'${moment(dailyOperation.createdUtc).add(7, "hours").format("YYYY-MM-DD")}'` : null,
                salesContractDeliveryDate: finishingPrintingSC ? `'${moment(finishingPrintingSC.deliverySchedule).add(7, "hours").format("YYYY-MM-DD")}'` : null,
                productionOrderDeliveryDate: productionOrder ? `'${moment(productionOrder.deliveryDate).add(7, "hours").format("YYYY-MM-DD")}'` : null
            }
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
                var sqlQuery = '';

                var count = 1;
                for (var item of data) {
                    if (item) {
                        var queryString = `INSERT INTO [dbo].[DL_Fact_Production_Order_Status_Temp]([salesContractDate], [salesContractNo], [salesContractQuantity], [productionOrderDate], [productionSalesContractNo], [productionOrderNo], [productionOrderQuantity], [kanbanDate], [kanbanSalesContractNo], [kanbanQuantity], [fabricQualityControlDate], [fabricQualityControlQuantity], [orderType], [deleted], [kanbanCode], [dailyOperationQuantity], [dailyOperationSalesContractNo], [dailyOperationCode], [fabricQualityControlCode], [cartNumber], [fabricqualitycontroltestindex], [dailyOperationDate], [salesContractDeliveryDate], [productionOrderDeliveryDate]) VALUES(${item.salesContractDate}, ${item.salesContractNo}, ${item.salesContractQuantity}, ${item.productionOrderDate}, ${item.productionSalesContractNo}, ${item.productionOrderNo}, ${item.productionOrderQuantity}, ${item.kanbanDate}, ${item.kanbanSalesContractNo}, ${item.kanbanQuantity}, ${item.fabricQualityControlDate}, ${item.fabricQualityControlQuantity}, ${item.orderType}, ${item.deleted}, ${item.kanbanCode}, ${item.dailyOperationQuantity}, ${item.dailyOperationSalesContractNo}, ${item.dailyOperationCode}, ${item.fabricQualityControlCode}, ${item.cartNumber}, ${item.fabricqualitycontroltestindex}, ${item.dailyOperationDate}, ${item.salesContractDeliveryDate}, ${item.productionOrderDeliveryDate});\n`;
                        sqlQuery = sqlQuery.concat(queryString);
                        if (count % 1000 == 0) {
                            command.push(insertQuery(sqlDWHConnections.sqlDWH, sqlQuery, t));
                            sqlQuery = "";
                        }
                        console.log(`add data to query  : ${count}`);
                        count++;
                    }
                }


                if (sqlQuery != "")
                    command.push(insertQuery(sqlDWHConnections.sqlDWH, `${sqlQuery}`, t));

                return Promise.all(command)
                    .then((results) => {
                        sqlDWHConnections.sqlDWH.query("exec DL_UPSERT_FACT_PRODUCTION_ORDER_STATUS", {
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