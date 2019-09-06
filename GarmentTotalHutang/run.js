let sqlDWHConnections = require('../Connection/DWH/');
let sqlFPConnection = require('../Connection/FinishingPrinting/');
let sqlSalesConnection = require('../Connection/Sales/');
let sqlPurchasingConnection = require('../Connection/Purchasing')

let moment = require('moment');


module.exports = async function () {
    return await extractFPSalesContract()
        .then((data) => transform(data))
        .then((data) => load(data));

}

const extractInternNote = async function () {
    var fpSalesContracts = await sqlPurchasingConnection
        .sqlPURCHASING
        .query(`select 
        g.isDeleted,
        g.inNo,
        g.inDate,
        g.supplierName,
        gid.pricePerDealUnit,
        gid.quantity
        from garmentinternnotes g left join GarmentInternNoteItems gi on g.Id = gi.GarmentINId left join GarmentInternNoteDetails gid on gi.Id = gid.GarmentItemINId`, {
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