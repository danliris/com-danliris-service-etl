let sqlDWHConnections = require('../Connection/DWH/');
let sqlFPConnection = require('../Connection/FinishingPrinting/')
let sqlCoreConnection = require('../Connection/Core/')
let sqlSalesConnection = require('../Connection/Sales/')
const MIGRATION_LOG_DESCRIPTION = "Fact Monitoring Event from MongoDB to Azure DWH";
let moment = require('moment');

module.exports = async function () {
    var startedDate = new Date();
    return await timestamp()
        .then((times) => extractMonitoringEvent(times))
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

const getOperationRange = function (hours) {
    return hours / 60;
}

const extractMonitoringEvent = async function (times) {
    var time = times.length > 0 ? moment(times[0].start).format("YYYY-MM-DD") : "1970-01-01";
    var timestamp = new Date(time);
    var monitoringEvent = await sqlFPConnection
        .sqlFP
        .query(`select cartNumber,
        code,
        dateStart,
        timeInMilisStart,
        timeInMilisEnd,
        dateEnd,
        productionOrderId,
        productionOrderOrderNo,
        remark,
        machineId,
        machineName,
        machineEventName,
        machineEventId,
        createdBy,
        isDeleted,
        machineEventCategory
        from monitoringevent
        where lastmodifiedutc >= :tanggal and CreatedBy not in (:creator)`, {
            replacements: { creator: ['dev', 'unit-test'], tanggal: timestamp },
            type: sqlFPConnection.sqlFP.QueryTypes.SELECT
        });

    for (var element of monitoringEvent) {
        element.machine = await joinMachine(element);
        element.productionOrder = await joinProductionOrder(element);
        element.machineEvent = await joinMachineEvent(element);
    }

    return monitoringEvent;
};

const joinMachine = async function (data) {

    var machineList = await sqlFPConnection
        .sqlFP
        .query(`select id, 
        code, 
        condition, 
        name, 
        manufacture, 
        process, 
        unitCode, 
        unitDivisionId, 
        unitDivisionName, 
        unitName, 
        year  
        from Machine where id = ?`, {
            replacements: [data.machineId],
            type: sqlFPConnection.sqlFP.QueryTypes.SELECT
        });

    var machine = machineList[0];
    if (machine) {
        machine.unit = {};
        machine.unit.code = machine.unitCode;
        machine.unit.name = machine.unitName;
        machine.unit.division = await joinDivision(machine);
    }

    return machine;
};

const joinDivision = async function (data) {

    var division = await sqlCoreConnection
        .sqlCore
        .query(`select id, code, name from divisions where id = ?`, {
            replacements: [data.unitDivisionId],
            type: sqlCoreConnection.sqlCore.QueryTypes.SELECT
        });


    return division[0];
};

const joinProductionOrder = async function (data) {

    var productionOrderList = await sqlSalesConnection
        .sqlSales
        .query(`SELECT
        buyerId,
        buyerCode,
        buyerName,
        buyerType,
        deliveryDate,
        designCode,
        designNumber,
        finishWidth,
        handlingStandard,
        materialName,
        orderQuantity,
        orderTypeName,
        processTypeName,
        remark,
        sample,
        shrinkageStandard,
        uomUnit,
        orderNo,
        RUN,
        salesContractNo
        FROM ProductionOrder where id = ?`, {
            replacements: [data.productionOrderId],
            type: sqlSalesConnection.sqlSales.QueryTypes.SELECT
        });

    var productionOrder = productionOrderList[0];
    if (productionOrder) {
        productionOrder.buyer = await joinBuyer(productionOrder);
        productionOrder.material = {};
        productionOrder.material.name = productionOrder.materialName;
        productionOrder.orderType = {};
        productionOrder.orderType.name = productionOrder.orderTypeName;
        productionOrder.processType = {};
        productionOrder.processType.name = productionOrder.processTypeName;
        productionOrder.uom = {};
        productionOrder.uom.unit = productionOrder.uomUnit;
    }

    return productionOrder;
};

const joinBuyer = async function (data) {

    var buyer = await sqlCoreConnection
        .sqlCore
        .query(`select address, code, contact, country, name, tempo from Buyers where id = ?`, {
            replacements: [data.buyerId],
            type: sqlCoreConnection.sqlCore.QueryTypes.SELECT
        });


    return buyer[0];
};

const joinMachineEvent = async function (data) {

    var machineEvent = await sqlFPConnection
        .sqlFP
        .query(`select id, no, name, category from MachineEvents where id = ?`, {
            replacements: [data.machineEventId],
            type: sqlFPConnection.sqlFP.QueryTypes.SELECT
        });


    return machineEvent[0];
};

const transform = function (data) {
    var result = data.map((item) => {
        var monitoringEvent = item;
        var time = "T";
        var ms = ".000Z";
        var startTime = moment(monitoringEvent.timeInMilisStart).format("HH:mm:ss");
        var endTime = moment(monitoringEvent.timeInMilisEnd).format("HH:mm:ss");
        var startDate = moment(monitoringEvent.dateStart).format("YYYY-MM-DD");
        var endDate = moment(monitoringEvent.dateEnd).format("YYYY-MM-DD");
        var start = moment(startDate.concat(time, startTime, ms)).format();
        var end = moment(endDate.concat(time, endTime, ms)).format();
        var operationRange = moment(end).diff(moment(start), "minutes");


        return {
            cartNumber: monitoringEvent.cartNumber ? `'${monitoringEvent.cartNumber}'` : null,
            monitoringEventCode: monitoringEvent.code ? `'${monitoringEvent.code}'` : null,
            monitoringEventStartedDate: monitoringEvent.dateStart ? `'${moment(monitoringEvent.dateStart).add(7, "hours").format("YYYY-MM-DD")}'` : null,
            eventStartedTime: monitoringEvent.timeInMilisStart ? `'${moment(monitoringEvent.timeInMilisStart).add(7, "hours").format("HH:mm:ss")}'` : null,
            eventEndTime: monitoringEvent.timeInMilisEnd ? `'${moment(monitoringEvent.timeInMilisEnd).add(7, "hours").format("HH:mm:ss")}'` : null,
            monitoringEventEndDate: monitoringEvent.dateEnd ? `'${moment(monitoringEvent.dateEnd).add(7, "hours").format("YYYY-MM-DD")}'` : null,
            machineCode: monitoringEvent.machine ? `'${monitoringEvent.machine.code}'` : null,
            machineCondition: monitoringEvent.machine ? `'${monitoringEvent.machine.condition}'` : null,
            machineManufacture: monitoringEvent.machine ? `'${monitoringEvent.machine.manufacture}'` : null,
            machineName: monitoringEvent.machine ? `'${monitoringEvent.machine.name.replace(/'/g, '"')}'` : null,
            machineProcess: monitoringEvent.machine ? `'${monitoringEvent.machine.process.replace(/'/g, '"')}'` : null,
            machineStepProcess: monitoringEvent.machine && monitoringEvent.machine.step && monitoringEvent.machine.step.process ? `'${monitoringEvent.machine.step.stepId}'` : null,
            unitCode: monitoringEvent.machine && monitoringEvent.machine.unit && monitoringEvent.machine.unit.code ? `'${monitoringEvent.machine.unit.code}'` : null,
            divisionCode: monitoringEvent.machine ? `'${monitoringEvent.machine.unit.division.code}'` : null,
            divisionName: monitoringEvent.machine ? `'${monitoringEvent.machine.unit.division.name}'` : null,
            unitName: monitoringEvent.machine ? `'${monitoringEvent.machine.unit.name}'` : null,
            machineYear: monitoringEvent.machine ? `${monitoringEvent.machine.year}` : null,
            productionOrderBuyerAddress: monitoringEvent.productionOrder ? `'${monitoringEvent.productionOrder.buyer.address}'` : null,
            productionOrderBuyerCode: monitoringEvent.productionOrder ? `'${monitoringEvent.productionOrder.buyer.code}'` : null,
            productionOrderBuyerContact: monitoringEvent.productionOrder ? `'${monitoringEvent.productionOrder.buyer.contact}'` : null,
            productionOrderBuyerCountry: monitoringEvent.productionOrder ? `'${monitoringEvent.productionOrder.buyer.country}'` : null,
            productionOrderBuyerName: monitoringEvent.productionOrder ? `'${monitoringEvent.productionOrder.buyer.name.replace(/'/g, '"')}'` : null,
            productionOrderBuyerTempo: monitoringEvent.productionOrder ? `'${monitoringEvent.productionOrder.buyer.tempo}'` : null,
            productionOrderConstruction: monitoringEvent.productionOrder && monitoringEvent.productionOrder.construction ? `'${monitoringEvent.productionOrder.construction}'` : null,
            productionOrderDeliveryDate: monitoringEvent.productionOrder ? `'${moment(monitoringEvent.productionOrder.deliveryDate).add(7, "hours").format("YYYY-MM-DD")}'` : null,
            productionOrderProductionOrderDesign: monitoringEvent.productionOrder && monitoringEvent.productionOrder.design ? `'${monitoringEvent.productionOrder.design}'` : null,
            productionOrderFinishWidth: monitoringEvent.productionOrder ? `'${monitoringEvent.productionOrder.finishWidth}'` : null,
            productionOrderHandlingStandard: monitoringEvent.productionOrder ? `'${monitoringEvent.productionOrder.handlingStandard}'` : null,
            productionOrderMaterial: monitoringEvent.productionOrder ? `'${monitoringEvent.productionOrder.material.name}'` : null,
            productionOrderOrderNo: monitoringEvent.productionOrder ? `'${monitoringEvent.productionOrder.orderNo}'` : null,
            productionOrderOrderQuantity: monitoringEvent.productionOrder ? `'${monitoringEvent.productionOrder.orderQuantity}'` : null,
            productionOrderOrderType: monitoringEvent.productionOrder ? `'${monitoringEvent.productionOrder.orderType.name}'` : null,
            productionOrderOriginGreigeFabric: monitoringEvent.productionOrder && monitoringEvent.productionOrder.originGreigeFabric ? `'${monitoringEvent.productionOrder.originGreigeFabric}'` : null,
            productionOrderProcessType: monitoringEvent.productionOrder ? `'${monitoringEvent.productionOrder.processType.name}'` : null,
            productionOrderRemark: monitoringEvent.productionOrder ? `'${monitoringEvent.productionOrder.remark}'` : null,
            productionOrderRollLength: monitoringEvent.productionOrder && monitoringEvent.productionOrder.rollLength ? `'${monitoringEvent.productionOrder.rollLength}'` : null,
            productionOrderRun: monitoringEvent.productionOrder ? `'${monitoringEvent.productionOrder.RUN}'` : null,
            productionOrderSalesContractNo: monitoringEvent.productionOrder ? `'${monitoringEvent.productionOrder.salesContractNo}'` : null,
            productionOrderSample: monitoringEvent.productionOrder ? `'${monitoringEvent.productionOrder.sample}'` : null,
            productionOrderShrinkageStandard: monitoringEvent.productionOrder ? `'${monitoringEvent.productionOrder.shrinkageStandard}'` : null,
            productionOrderSpelling: monitoringEvent.productionOrder && monitoringEvent.productionOrder.spelling ? `${monitoringEvent.productionOrder.spelling}` : null,
            productionOrderUom: monitoringEvent.productionOrder ? `'${monitoringEvent.productionOrder.uom.unit}'` : null,
            monitoringEventRemark: monitoringEvent.remark ? `'${monitoringEvent.remark.replace(/'/g, '"')}'` : null,
            selectedProductionOrderDetailCode: null,
            selectedProductionOrderDetailColorRequest: null,
            selectedProductionOrderDetailColorTemplate: null,
            selectedProductionOrderDetailColorTypeCode: null,
            selectedProductionOrderDetailColorTypeName: null,
            selectedProductionOrderDetailColorTypeRemark: null,
            selectedProductionOrderDetailQuantity: null,
            selectedProductionOrderDetailUom: null,
            machineEventName: (monitoringEvent && monitoringEvent.machineEvent && monitoringEvent.machineEvent.name) ? `'${monitoringEvent.machineEvent.name.replace(/'/g, '"')}'` : null,
            eventRange: monitoringEvent.dateEnd ? `'${getOperationRange(operationRange)}'` : null,
            machineEventNo: (monitoringEvent.machineEvent && monitoringEvent.machineEvent.no) ? `'${monitoringEvent.machineEvent.no.toString().replace(/'/g, '"')}'` : null,
            createdBy: monitoringEvent ? `'${monitoringEvent.createdBy}'` : null,
            deleted: `'${monitoringEvent.isDeleted}'`,
            eventCategory: (monitoringEvent.machineEvent && monitoringEvent.machineEvent.category && monitoringEvent.machineEvent.category !== "") ? `'${monitoringEvent.machineEvent.category}'` : null
        }
    });
    return Promise.resolve([].concat.apply([], result))
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
                        var queryString = `INSERT INTO [dbo].[DL_Fact_Monitoring_Event_Temp]([cartNumber], [monitoringEventCode], [monitoringEventStartedDate], [eventStartedTime], [monitoringEventEndDate], [eventEndTime], [machineCode], [machineName], [machineProcess], [machineStepProcess], [unitCode], [divisionCode], [divisionName], [unitName], [productionOrderBuyerName], [productionOrderConstruction], [productionOrderDetailCode], [productionOrderDetailColorRequest], [productionOrderDetailColorTemplate], [productionOrderDetailColorTypeName], [productionOrderOrderType], [productionOrderProcessType], [productionOrderSalesContractNo], [monitoringEventRemark], [selectedProductionOrderDetailCode], [selectedProductionOrderDetailColorRequest], [selectedProductionOrderDetailColorTemplate], [selectedProductionOrderDetailColorTypeName], [machineEventName], [eventRange], [productionOrderOrderNo], [machineEventNo], [createdBy], [deleted], [eventCategory]) VALUES(${item.cartNumber}, ${item.monitoringEventCode}, ${item.monitoringEventStartedDate}, ${item.eventStartedTime}, ${item.monitoringEventEndDate}, ${item.eventEndTime}, ${item.machineCode}, ${item.machineName}, ${item.machineProcess}, ${item.machineStepProcess}, ${item.unitCode}, ${item.divisionCode}, ${item.divisionName}, ${item.unitName}, ${item.productionOrderBuyerName}, ${item.productionOrderConstruction}, ${item.selectedProductionOrderDetailCode}, ${item.selectedProductionOrderDetailColorRequest}, ${item.selectedProductionOrderDetailColorTemplate}, ${item.selectedProductionOrderDetailColorTypeName}, ${item.productionOrderOrderType}, ${item.productionOrderProcessType}, ${item.productionOrderSalesContractNo}, ${item.monitoringEventRemark}, ${item.selectedProductionOrderDetailCode}, ${item.selectedProductionOrderDetailColorRequest}, ${item.selectedProductionOrderDetailColorTemplate}, ${item.selectedProductionOrderDetailColorTypeName}, ${item.machineEventName}, ${item.eventRange}, ${item.productionOrderOrderNo}, ${item.machineEventNo}, ${item.createdBy}, ${item.deleted}, ${item.eventCategory});\n`;
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
                        sqlDWHConnections.sqlDWH.query("exec DL_UPSERT_FACT_MONITORING_EVENT", {
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