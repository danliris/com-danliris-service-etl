let sqlDWHConnections = require('../Connection/DWH/');
let sqlFPConnection = require('../Connection/FinishingPrinting/')
const MIGRATION_LOG_DESCRIPTION = "Fact Shipment Document from MongoDB to Azure DWH";
let moment = require('moment');

module.exports = async function () {
    var startedDate = new Date();
    return await timestamp()
        .then((times) => joinShipmentDocument(times))
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
}

const extractShipmentDocument = async function (times) {
    var time = times.length > 0 ? moment(times[0].start).format("YYYY-MM-DD") : "1970-01-01";
    var timestamp = new Date(time);
    return sqlFPConnection
        .sqlFP
        .query(`select
        id,
        buyerCode,
        buyerName,
        buyerType,
        code,
        deliveryDate,
        isVoid
        from ShipmentDocuments
        where lastmodifiedutc > ?`, {
            replacements: [timestamp],
            type: sqlFPConnection.sqlFP.QueryTypes.SELECT
        });

};

const extractShipmentDocumentDetails = async function () {
    return sqlFPConnection
        .sqlFP
        .query(`select
        id,
        shipmentDocumentId,
        ProductionOrderDesignCode designCode,
        ProductionOrderDesignNumber designNumber,
        productionOrderType,
        productionOrderNo
        from shipmentDocumentDetails `, {
            type: sqlFPConnection.sqlFP.QueryTypes.SELECT
        });

};

const extractShipmentDocumentItems = async function () {
    return sqlFPConnection
        .sqlFP
        .query(`select
        id,
        shipmentDocumentDetailId
        from shipmentDocumentItems`, {
            type: sqlFPConnection.sqlFP.QueryTypes.SELECT
        });

};

const extractShipmentDocumentPackingReceiptItems = async function () {
    return sqlFPConnection
        .sqlFP
        .query(`select
        id,
        shipmentDocumentItemId,
        colorType,
        length,
        productCode,
        productName,
        quantity,
        uomUnit,
        weight
        from ShipmentDocumentPackingReceiptItems`, {
            type: sqlFPConnection.sqlFP.QueryTypes.SELECT
        });

};

const joinShipmentDocument = function (times) {
    var shipmentDocuments = extractShipmentDocument(times);
    var shipmentDocumentDetails = extractShipmentDocumentDetails();
    var shipmentDocumentItems = extractShipmentDocumentItems();
    var shipmentDocumentPackingReceiptItems = extractShipmentDocumentPackingReceiptItems();

    return Promise.all([shipmentDocuments, shipmentDocumentDetails, shipmentDocumentItems, shipmentDocumentPackingReceiptItems])
        .then((data) => {
            var shipmentDocuments = data[0];
            var shipmentDocumentDetails = data[1];
            var shipmentDocumentItems = data[2];
            var shipmentDocumentPackingReceiptItems = data[3];

            for (var element of shipmentDocuments) {
                element.details = shipmentDocumentDetails.filter(x => x.shipmentDocumentId == element.id);
                for (var detail of element.details) {
                    detail.items = shipmentDocumentItems.filter(x => x.shipmentDocumentDetailId == detail.id);
                    for (var item of detail.items) {
                        item.packingReceiptItems = shipmentDocumentPackingReceiptItems.filter(x => x.shipmentDocumentItemId == item.id);
                    }
                }
            }

            return Promise.resolve(shipmentDocuments);
        });
};

function transform(shipments) {

    var result = [];

    for (var shipment of shipments) {

        if (shipment.details && shipment.details.length > 0) {
            for (var detail of shipment.details) {

                if (detail.items && detail.items.length > 0) {
                    for (var item of detail.items) {
                        if (item.packingReceiptItems && item.packingReceiptItems.length > 0) {
                            for (var packingReceiptItem of item.packingReceiptItems) {
                                var obj = {
                                    buyerCode: shipment.buyerCode ? `'${shipment.buyerCode.replace(/'/g, '"')}'` : null,
                                    buyerName: shipment.buyerName ? `'${shipment.buyerName.replace(/'/g, '"')}'` : null,
                                    buyerType: shipment.buyerType ? `'${shipment.buyerType.replace(/'/g, '"')}'` : null,
                                    shipmentCode: shipment.code ? `'${shipment.code.replace(/'/g, '"')}'` : null,
                                    deliveryDate: shipment.deliveryDate ? `'${moment(shipment.deliveryDate).format("YYYY-MM-DD")}'` : null,
                                    isVoid: `'${shipment.isVoid}'`,
                                    designCode: detail.designCode ? `'${detail.designCode.replace(/'/g, '"')}'` : null,
                                    designNumber: detail.designNumber ? `'${detail.designNumber.replace(/'/g, '"')}'` : null,
                                    productionOrderNo: detail.productionOrderNo ? `'${detail.productionOrderNo.replace(/'/g, '"')}'` : null,
                                    productionOrderType: detail.productionOrderType ? `'${detail.productionOrderType.replace(/'/g, '"')}'` : null,
                                    colorType: packingReceiptItem.colorType ? `'${packingReceiptItem.colorType.replace(/'/g, '"')}'` : null,
                                    length: packingReceiptItem.length != undefined ? `${packingReceiptItem.length}` : null,
                                    productCode: packingReceiptItem.productCode ? `'${packingReceiptItem.productCode.replace(/'/g, '"')}'` : null,
                                    productName: packingReceiptItem.productName ? `'${packingReceiptItem.productName.replace(/'/g, '"')}'` : null,
                                    quantity: packingReceiptItem.quantity != undefined ? `${packingReceiptItem.quantity}` : null,
                                    uomUnit: packingReceiptItem.uomUnit ? `'${packingReceiptItem.uomUnit.replace(/'/g, '"')}'` : null,
                                    weight: packingReceiptItem.weight != undefined ? `${packingReceiptItem.weight}` : null
                                }

                                result.push(obj);
                            }
                        }
                    }
                }
            }
        }
    }

    return Promise.resolve(result);
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
                var sqlQuery = 'INSERT INTO [DL_Fact_Shipment_Document_Temp] ';

                var count = 1;
                for (var item of data) {
                    if (item) {
                        var queryString = `\nSELECT ${item.buyerCode}, ${item.buyerName}, ${item.buyerType}, ${item.shipmentCode}, ${item.deliveryDate}, ${item.isVoid}, ${item.designCode}, ${item.designNumber}, ${item.productionOrderNo}, ${item.productionOrderType}, ${item.colorType}, ${item.length}, ${item.productCode}, ${item.productName}, ${item.quantity}, ${item.uomUnit}, ${item.weight} UNION ALL `;
                        sqlQuery = sqlQuery.concat(queryString);
                        if (count % 1000 == 0) {
                            sqlQuery = sqlQuery.substring(0, sqlQuery.length - 10);
                            command.push(insertQuery(sqlDWHConnections.sqlDWH, sqlQuery, t));
                            sqlQuery = "INSERT INTO [DL_Fact_Shipment_Document_Temp] ";
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
                        sqlDWHConnections.sqlDWH.query("exec [DL_UPSERT_FACT_SHIPMENT_DOCUMENT]", {
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