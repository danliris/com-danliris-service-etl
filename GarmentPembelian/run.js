let sqlDWHConnections = require('../Connection/DWH/');
let sqlCoreConnection = require('../Connection/Core/');
let sqlPurchasingConnection = require('../Connection/Purchasing');
const MIGRATION_LOG_DESCRIPTION = "Fact Pembelian Garment from MongoDB to Azure DWH";
const minimumDateString = "1753-01-01";
let moment = require('moment');

module.exports = async function () {
    var startedDate = new Date();
    return await timestamp()
        .then((times) => getGarmentPurchaseRequests(times))
        .then((data) => joinPurchaseOrder(data))
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
            // return updateLog;
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

const garmentPR = function (times) {
    var time = times.length > 0 ? moment(times[0].start).format("YYYY-MM-DD") : "1970-01-01";
    var timestamp = new Date(time);
    return sqlPurchasingConnection
        .sqlPURCHASING
        .query(`select 
        CreatedUtc,
        CreatedBy,
        IsDeleted,
        PRNo,
        RONo,
        BuyerCode,
        BuyerName,
        Article,
        Date,
        ExpectedDeliveryDate,
        ShipmentDate,
        UnitCode,
        UnitName,
        UnitId,
        IsPosted,
        IsUsed,
        Id
        from garmentpurchaserequests
        where lastmodifiedutc >= :tanggal and CreatedBy not in (:creator)
        `, {
            replacements: { creator: ['dev', 'unit-test'], tanggal: timestamp },
            type: sqlPurchasingConnection.sqlPURCHASING.QueryTypes.SELECT
        });
};
const garmentPRItems = async function (data) {
    return await sqlPurchasingConnection
        .sqlPURCHASING
        .query(`select
        PO_SerialNumber,
        ProductCode,
        ProductName,
        quantity,
        BudgetPrice,
        UomUnit,
        CategoryId,
        CategoryName,
        IsUsed,
        GarmentPRId,
        Id
        from GarmentPurchaseRequestItems where GarmentPRId = ?`, {
            replacements: [data.Id],
            type: sqlPurchasingConnection.sqlPURCHASING.QueryTypes.SELECT
        });
};
const division = function () {
    return sqlCoreConnection
        .sqlCore
        .query(`select 
        id,
        DivisionId,
        DivisionName,
        DivisionCode 
        from units`, {
            type: sqlCoreConnection.sqlCore.QueryTypes.SELECT
        });
};

const garmentCategories = function () {
    return sqlCoreConnection
        .sqlCore
        .query(`select
        Id,
        Code,
        Name
        from GarmentCategories`, {
            type: sqlCoreConnection.sqlCore.QueryTypes.SELECT
        });
};

const getCurrencies = function () {
    return sqlCoreConnection
        .sqlCore
        .query(`select
        id,
        code,
        rate,
        symbol
        from currencies`, {
            type: sqlCoreConnection.sqlCore.QueryTypes.SELECT
        });
};

const getGarmentPurchaseRequests = function (times) {
    var pr = garmentPR(times);
    var div = division();
    var categories = garmentCategories();
    return Promise.all([pr, div, categories])
        .then(async (data) => {
            var pr = data[0];
            var div = data[1];
            var categories = data[2];

            var results = [];

            for (var item of pr) {
                var result = {};
                result._createdBy = item.CreatedBy;
                result._createdDate = item.CreatedUtc;
                result._deleted = item.IsDeleted;
                result.no = item.PRNo;
                result.roNo = item.RONo;
                result.buyer = {};
                result.buyer.code = item.BuyerCode;
                result.buyer.name = item.BuyerName;
                result.artikel = item.Article;
                result.date = item.Date;
                result.expectedDeliveryDate = item.ExpectedDeliveryDate;
                result.shipmentDate = item.ShipmentDate;
                result.unit = {};
                result.unit.code = item.UnitCode;
                result.unit.name = item.UnitName;
                result.unit.division = {};

                var resultDiv = div.find(x => x.id == item.UnitId);
                if (resultDiv) {
                    result.unit.division.code = resultDiv.DivisionCode;
                    result.unit.division.name = resultDiv.DivisionName;
                }

                result.isPosted = item.IsPosted;
                result.isUsed = item.IsUsed;
                var prItems = await garmentPRItems(item);
                result.items = prItems
                    .map(data => {
                        var resultCategory = categories.find(x => x.Id == data.CategoryId);

                        return {
                            product: {
                                code: data.ProductCode,
                                name: data.ProductName
                            },
                            quantity: data.quantity,
                            budgetPrice: data.BudgetPrice,
                            uom: {
                                unit: data.UomUnit
                            },
                            category: {
                                code: resultCategory ? resultCategory.Code : null,
                                name: data.CategoryName
                            },
                            isUsed: data.IsUsed
                        };
                    });
                result.Id = item.Id;
                results.push(result);
            }

            return Promise.resolve(results);
        });
};

const joinPurchaseOrder = async function (purchaseRequests) {
    let result = [];
    let allFilteredPuchaseOrders = await getPurchaseOrder(purchaseRequests);
    for (var purchaseRequest of purchaseRequests) {
        let filteredPurchaseOrders = allFilteredPuchaseOrders.filter(x => x.PRId == purchaseRequest.Id);
        let dataToPush = {};
        if (filteredPurchaseOrders.length > 0) {
            for (let purchaseOrder of filteredPurchaseOrders) {
                dataToPush = {
                    purchaseRequest: purchaseRequest,
                    purchaseOrder: purchaseOrder
                }
                result.push(dataToPush);
            }
        } else {
            dataToPush = {
                purchaseRequest: purchaseRequest,
                purchaseOrder: null
            }
            result.push(dataToPush);
        }


    }
    return result;
}

const getPurchaseOrder = function (purchaseRequests) {
    var poInternal = getPOInternal(purchaseRequests);
    var div = division();
    var currencies = getCurrencies();
    var categories = garmentCategories();
    return Promise.all([poInternal, div, currencies, categories])
        .then(async (data) => {
            var poInternal = data[0];
            var div = data[1];
            var currencies = data[2];
            var categories = data[3];
            var results = [];
            var allPoItems = await getPOInternalItems(poInternal, currencies, categories);
            for (var item of poInternal) {
                var result = {};
                result._createdBy = item.CreatedBy;
                result._createdDate = item.CreatedUtc;
                result._deleted = item.IsDeleted;
                result.no = item.PONo;
                result.purchaseRequestId = item.PRId;
                result.roNo = item.RONo;
                result.buyer = {};
                result.buyer.code = item.BuyerCode;
                result.buyer.name = item.BuyerName;
                result.artikel = item.Article;
                result.unit = {};
                result.unit.code = item.UnitCode;
                result.unit.name = item.UnitName;
                result.unit.division = {};
                result.PRId = item.PRId;

                var resultDiv = div.find(x => x.id == item.UnitId);
                if (resultDiv) {
                    result.unit.division.code = resultDiv.DivisionCode;
                }

                result.date = item.PRDate;
                result.expectedDeliveryDate = item.ExpectedDeliveryDate;
                result.shipmentDate = item.ShipmentDate;
                result.isPosted = item.IsPosted;
                result.isClosed = item.IsClosed;

                var poItems = allPoItems.filter(x => x.GPOId == item.Id);
                if (poItems) {
                    result.items = poItems;
                }
                results.push(result);
            }
            return Promise.resolve(results);
        });
}

const getPOInternal = function (data) {
    var dataIds = data.map(x => x.Id);
    return sqlPurchasingConnection
        .sqlPURCHASING
        .query(`select
        CreatedBy,
        CreatedUtc,
        IsDeleted,
        PONo,
        PRId,
        RONo,
        BuyerCode,
        BuyerName,
        Article,
        UnitCode,
        UnitName,
        UnitId,
        PRDate,
        ExpectedDeliveryDate,
        ShipmentDate,
        IsPosted,
        IsClosed,
        Id
        from GarmentInternalPurchaseOrders
        where prid in (:dataId) and CreatedBy not in (:creator)`, {
            replacements: { creator: ['dev', 'unit-test'], dataId: dataIds },
            type: sqlPurchasingConnection.sqlPURCHASING.QueryTypes.SELECT
        });
}

const getPOInternalItems = async function (poInternals, currencies, categories) {
    var ids = poInternals.map(x => x.Id);
    var poInternalItems = await sqlPurchasingConnection
        .sqlPURCHASING
        .query(`select
        ProductCode,
        ProductName,
        BudgetPrice,
        CategoryId,
        CategoryName,
        Status,
        GPOId,
        Id
        from GarmentInternalPurchaseOrderItems
        where GPOId in (:ids)`, {
            replacements: { ids: ids },
            type: sqlPurchasingConnection.sqlPURCHASING.QueryTypes.SELECT
        });
    var allPoExternals = await getPOExternal(poInternalItems);
    var allFulfillments = await getFulfillments(allPoExternals);
    for (var item of poInternalItems) {
        item.product = {};
        item.product.code = item.ProductCode;
        item.product.name = item.ProductName;
        item.budgetPrice = item.BudgetPrice;
        item.category = {};
        item.category.name = item.CategoryName;
        var garmentCat = categories.find(x => x.Id == item.CategoryId);
        if (garmentCat) {
            item.category.code = garmentCat.Code;
        }

        item.status = {};
        item.status.name = item.Status;
        item.id_po = item.GPOId;

        var poExternal = allPoExternals.find(x => x.POId == item.GPOId);
        if (poExternal) {
            item.purchaseOrderExternal = poExternal;
            item.purchaseOrderExternal._createdDate = poExternal.CreatedUtc;
            item.purchaseOrderExternal.no = poExternal.EPONo;
            item.purchaseOrderExternal.paymentType = poExternal.PaymentType;
            item.purchaseOrderExternal.isPosted = poExternal.IsPosted;
            item.purchaseOrderExternal.isClosed = poExternal.IsClosed;


            item.defaultQuantity = poExternal.DefaultQuantity;
            item.defaultUom = {};
            item.defaultUom.unit = poExternal.DefaultUomUnit;
            item.dealQuantity = poExternal.DealQuantity;
            item.dealUom = {};
            item.dealUom.unit = poExternal.DealUomUnit;
            item.pricePerDealUnit = poExternal.PricePerDealUnit;
            item.currency = {};
            item.currency.code = poExternal.CurrencyCode;
            item.currency.rate = poExternal.CurrencyRate;
            var currency = currencies.find(x => x.code == item.currency.code);
            if (currency) {
                item.currency.symbol = currency.symbol;
            }
            var fulfillments = allFulfillments.filter(x => x.EPOItemId == poExternal.Id);
            if (fulfillments) {
                item.fulfillments = fulfillments;
            }
            item.conversion = poExternal.Conversion;
            item.isPosted = poExternal.IsPosted;
            item.isClosed = poExternal.IsClosed;
            item.supplier = {};
            item.supplier.code = poExternal.SupplierCode;
            item.supplier.name = poExternal.SupplierName;
            item.freightCostBy = poExternal.FreightCostBy;
            item.paymentMethod = poExternal.PaymentMethod;
            item.paymentDueDays = poExternal.PaymentDueDays;
            item.useVat = poExternal.IsUseVat;
            item.useIncomeTax = poExternal.IsIncomeTax;


        } else {
            item.purchaseOrderExternal = null;
        }

    }

    return poInternalItems;
};

const getPOExternal = async function (poInternals) {
    var gpoIds = poInternals.map(x => x.GPOId);
    var poExternals = await sqlPurchasingConnection
        .sqlPURCHASING
        .query(`select
        ei.DefaultQuantity,
        ei.DefaultUomUnit,
        ei.DealQuantity,
        ei.DealUomUnit,
        ei.PricePerDealUnit,
        e.CurrencyCode,
        e.CurrencyRate,
        e.CurrencyId,
        ei.Conversion,
        e.IsPosted,
        e.IsClosed,
        e.CreatedUtc,
        e.EPONo,
        e.PaymentType,
        e.OrderDate,
        e.DeliveryDate,
        e.SupplierCode,
        e.SupplierName,
        e.FreightCostBy,
        e.PaymentMethod,
        e.PaymentDueDays,
        e.IsUseVat,
        e.IsIncomeTax,
        ei.Id,
        ei.POId
        from GarmentExternalPurchaseOrderItems ei left join GarmentExternalPurchaseOrders e on ei.GarmentEPOId = e.Id        
        where ei.POId in (:poid) and ei.CreatedBy not in (:creator)`, {
            replacements: { creator: ['dev', 'unit-test'], poid: gpoIds },
            type: sqlPurchasingConnection.sqlPURCHASING.QueryTypes.SELECT
        });
    // var poExternal = poExternals[0];
    return poExternals;
};

const getFulfillments = async function (poExternalItems) {
    var ids = poExternalItems.map(x => x.Id);
    var fulfillments = await sqlPurchasingConnection
        .sqlPURCHASING
        .query(`select
        gd.DOQuantity,
        g.DONo,
        g.DODate,
        gd.Id DetailId,
        g.Id,
        gd.EPOItemId
        from GarmentDeliveryOrderDetails gd left join GarmentDeliveryOrderItems gi on gd.GarmentDOItemId = gi.Id left join GarmentDeliveryOrders g on gi.GarmentDOId = g.Id     
        where gd.EPOItemId in (:ids)`, {
            replacements: { ids: ids },
            type: sqlPurchasingConnection.sqlPURCHASING.QueryTypes.SELECT
        });
    var allUrn = await getURN(fulfillments);
    var allInternNote = await getInternNote(fulfillments);
    for (var item of fulfillments) {
        item.deliveryOrderNo = item.DONo;
        item.deliveryOrderDate = item.DODate;
        item.deliveryOrderDeliveredQuantity = item.DOQuantity;
        var urn = allUrn.find(x => x.DODetailId == item.DetailId);
        if (urn) {
            item.unitReceiptNoteNo = urn.URNNo;
            item.unitReceiptNoteDate = urn.ReceiptDate;
            item.unitReceiptNoteDeliveredQuantity = urn.OrderQuantity;
            item.unitReceiptNoteDeliveredUomUnit = urn.UomUnit;
        }
        var internNote = allInternNote.find(x => x.DOId == item.Id);
        if (internNote) {
            item.interNoteNo = internNote.INNo;
            item.interNoteDate = internNote.INDate;
            item.interNotePrice = internNote.PriceTotal;
            item.interNoteQuantity = internNote.Quantity;
            item.interNoteDueDate = internNote.PaymentDueDate;
        }
    }

    return fulfillments;
}

const getURN = async function (deliveryOrders) {
    var ids = deliveryOrders.map(x => x.DetailId);
    var urns = await sqlPurchasingConnection
        .sqlPURCHASING
        .query(`select 
        g.URNNo,
        g.ReceiptDate,
        gi.OrderQuantity,
        gi.UomUnit,
        gi.DODetailId
        from GarmentUnitReceiptNoteItems gi left join GarmentUnitReceiptNotes g on gi.URNId = g.Id
        where gi.DODetailId in (:ids)`, {
            replacements: { ids: ids },
            type: sqlPurchasingConnection.sqlPURCHASING.QueryTypes.SELECT
        });
    // var urn = urns[0];
    return urns;
};

const getInternNote = async function (deliveryOrders) {
    var ids = deliveryOrders.map(x => x.Id);
    var ins = await sqlPurchasingConnection
        .sqlPURCHASING
        .query(`select
        g.INNo,
        g.INDate,
        gd.PriceTotal,
        gd.Quantity,
        gd.PaymentDueDate,
        gd.DOId
        from GarmentInternNoteDetails gd left join GarmentInternNoteItems gi on gd.GarmentItemINId = gi.Id left join GarmentInternNotes g on gi.GarmentINId = g.Id
        where gd.DOId in (:ids)`, {
            replacements: { ids: ids },
            type: sqlPurchasingConnection.sqlPURCHASING.QueryTypes.SELECT
        });
    // var interNote = ins[0];
    return ins;
};

function getRangeMonth(days) {
    if (days <= 30) {
        return "0-30 hari";
    } else if (days >= 31 && days <= 60) {
        return "31-60 hari";
    } else if (days >= 61 && days <= 90) {
        return "61-90 hari";
    } else if (days > 90) {
        return ">90 hari";
    } else {
        return "";
    }
};

function getRangeWeek(days) {
    if (days <= 7) {
        return "0-7 hari";
    } else if (days >= 8 && days <= 14) {
        return "8-14 hari";
    } else if (days >= 15 && days <= 30) {
        return "15-30 hari";
    } else if (days > 30) {
        return ">30 hari";
    } else {
        return "";
    }
};

function getCategoryType(categoryCode) {
    var categoryList = ["emb", "wsh", "pls", "prn", "tes", "qlt"];
    var found = categoryList.find((category) => category === categoryCode.toString().toLowerCase());
    if (categoryCode.toString().toLowerCase() === "fab") {
        return "Bahan Baku";
    } else if (found) {
        return "Jasa";
    } else {
        return "Accessories";
    }
}

function getStatus(poDate, doDate) {
    var result = moment(moment(doDate).add(7, "h").startOf("day")).diff(moment(moment(poDate).add(7, "h").startOf("day")), "days")
    if (result <= 0) {
        return "Tepat Waktu";
    } else {
        return "Tidak Tepat Waktu";
    }
}

function validateDate(date) {
    let minimumDate = new Date(minimumDateString);
    let dateToValidate = new Date(date);
    let dateNow = new Date();

    if (dateToValidate < minimumDate) {
        dateToValidate = new Date(dateToValidate.setFullYear(dateNow.getFullYear()))
    }

    return dateToValidate;
}

function transform(objects) {
    var result = objects.map((object) => {
        var purchaseRequest = object.purchaseRequest;
        var purchaseOrder = object.purchaseOrder;

        if (purchaseOrder) {

            var results = purchaseOrder.items.map((poItem) => {
                // var catType = (purchaseRequest.category && purchaseRequest.category.name) ? purchaseRequest.category.name : null;

                if (poItem.fulfillments && poItem.fulfillments.length > 0) {

                    return poItem.fulfillments.map((poFulfillment) => {
                        var prPoExtDays = (poItem.purchaseOrderExternal && poItem.purchaseOrderExternal._createdDate) ? moment(moment(validateDate(poItem.purchaseOrderExternal._createdDate)).add(7, "h").startOf("day")).diff(moment(moment(validateDate(purchaseRequest._createdDate)).add(7, "h").startOf("day")), "days") : null;
                        var poExtDays = (poItem.purchaseOrderExternal && poItem.purchaseOrderExternal._createdDate) ? moment(moment(validateDate(poItem.purchaseOrderExternal._createdDate)).add(7, "h").startOf("day")).diff(moment(moment(validateDate(purchaseOrder._createdDate)).add(7, "h").startOf("day")), "days") : null;
                        var poIntDays = purchaseOrder._createdDate ? moment(moment(validateDate(purchaseOrder._createdDate)).add(7, "h").startOf("day")).diff(moment(moment(validateDate(purchaseRequest._createdDate)).add(7, "h").startOf("day")), "days") : null;
                        var doDays = (poFulfillment.deliveryOrderDate && poItem.purchaseOrderExternal && poItem.purchaseOrderExternal._createdDate) ? moment(moment(validateDate(poFulfillment.deliveryOrderDate)).add(7, "h").startOf("day")).diff(moment(moment(validateDate(poItem.purchaseOrderExternal._createdDate)).add(7, "h").startOf("day")), "days") : null;
                        var urnDays = poFulfillment.unitReceiptNoteDate ? moment(moment(validateDate(poFulfillment.unitReceiptNoteDate)).add(7, "h").startOf("day")).diff(moment(moment(validateDate(poFulfillment.deliveryOrderDate)).add(7, "h").startOf("day")), "days") : null;
                        var upoDays = poFulfillment.interNoteDate ? moment(moment(validateDate(poFulfillment.interNoteDate)).add(7, "h").startOf("day")).diff(moment(moment(validateDate(poFulfillment.unitReceiptNoteDate)).add(7, "h").startOf("day")), "days") : null;
                        var poDays = poFulfillment.interNoteDate ? moment(moment(validateDate(poFulfillment.interNoteDate)).add(7, "h").startOf("day")).diff(moment(moment(validateDate(purchaseOrder._createdDate)).add(7, "h").startOf("day")), "days") : null;
                        var lastDeliveredDate = poFulfillment.deliveryOrderDate ? poItem.fulfillments[poItem.fulfillments.length - 1].deliveryOrderDate : null;

                        return {
                            purchaseRequestNo: purchaseRequest.no ? `'${purchaseRequest.no.replace(/'/g, '"')}'` : null, //Nomor PR
                            purchaseRequestDate: purchaseRequest._createdDate ? `'${moment(validateDate(purchaseRequest._createdDate)).add(7, "h").format('YYYY-MM-DD')}'` : null, //Tanggal PR
                            expectedPRDeliveryDate: purchaseRequest.expectedDeliveryDate ? `'${moment(validateDate(purchaseRequest.expectedDeliveryDate)).add(7, "h").format('YYYY-MM-DD')}'` : null, //Tanggal Diminta Datang
                            unitCode: (purchaseRequest.unit && purchaseRequest.unit.code) ? `'${purchaseRequest.unit.code.replace(/'/g, '"')}'` : null, //Kode Unit
                            unitName: (purchaseRequest.unit && purchaseRequest.unit.name) ? `'${purchaseRequest.unit.name.replace(/'/g, '"')}'` : null, //Nama Unit
                            divisionCode: (purchaseRequest.unit && purchaseRequest.unit.division && purchaseRequest.unit.division.code) ? `'${purchaseRequest.unit.division.code.replace(/'/g, '"')}'` : null, //Kode Divisi
                            divisionName: (purchaseRequest.unit && purchaseRequest.unit.division && purchaseRequest.unit.division.name) ? `'${purchaseRequest.unit.division.name.replace(/'/g, '"')}'` : null, //Nama Divisi
                            categoryCode: (poItem.category && poItem.category.code) ? `'${poItem.category.code.replace(/'/g, '"')}'` : null, //Kode Kategori
                            categoryName: (poItem.category && poItem.category.name) ? `'${poItem.category.name.replace(/'/g, '"')}'` : null, //Nama Kategori
                            categoryType: (poItem.category && poItem.category.code) ? `'${getCategoryType(poItem.category.code.replace(/'/g, '"'))}'` : null, //Jenis Kategori
                            productCode: (poItem.product && poItem.product.code) ? `'${poItem.product.code.replace(/'/g, '"')}'` : null, //Kode Produk
                            productName: (poItem.product && poItem.product.name) ? `'${poItem.product.name.replace(/'/g, '"')}'` : null, //Nama Produk
                            purchaseRequestDays: `${!isNaN(poIntDays) ? poIntDays : 0}`, //Jumlah Selisih Hari PR-PO Internal
                            purchaseRequestDaysRange: poIntDays !== null ? `'${getRangeWeek(poIntDays)}'` : null, //Selisih Hari PR-PO Internal
                            prPurchaseOrderExternalDays: `${!isNaN(prPoExtDays) ? prPoExtDays : 0}`, //Jumlah Selisih Hari PR-PO Eksternal
                            prPurchaseOrderExternalDaysRange: prPoExtDays !== null ? `'${getRangeWeek(prPoExtDays)}'` : null, //Selisih Hari PR-PO Eksternal
                            deletedPR: `'${purchaseRequest._deleted}'`,

                            purchaseOrderNo: purchaseOrder.no ? `'${purchaseOrder.no.replace(/'/g, '"')}'` : null, //Nomor PO Internal
                            purchaseOrderDate: purchaseOrder._createdDate ? `'${moment(validateDate(purchaseOrder._createdDate)).add(7, "h").format('YYYY-MM-DD')}'` : null, //Tanggal PO Internal
                            purchaseOrderExternalDays: `${!isNaN(poExtDays) ? poExtDays : 0}`, //Jumlah Selisih Hari PO Internal-PO Eksternal
                            purchaseOrderExternalDaysRange: poExtDays !== null ? `'${getRangeWeek(poExtDays)}'` : null, //Selisih Hari PO Internal-PO Eksternal
                            purchasingStaffName: purchaseOrder._createdBy ? `'${purchaseOrder._createdBy.replace(/'/g, '"')}'` : null, //Nama Staff Pembelian
                            prNoAtPo: purchaseRequest.no ? `'${purchaseRequest.no.replace(/'/g, '"')}'` : null, //Nomor PR di PO Internal
                            deletedPO: `'${purchaseOrder._deleted}'`,

                            purchaseOrderExternalNo: (poItem.purchaseOrderExternal && poItem.purchaseOrderExternal.no) ? `'${poItem.purchaseOrderExternal.no.replace(/'/g, '"')}'` : null, // Nomor PO Eksternal
                            purchaseOrderExternalDate: (poItem.purchaseOrderExternal && poItem.purchaseOrderExternal._createdDate) ? `'${moment(validateDate(poItem.purchaseOrderExternal._createdDate)).add(7, "h").format('YYYY-MM-DD')}'` : null, //Tanggal PO Eksternal
                            deliveryOrderDays: poFulfillment.deliveryOrderDate ? `${!isNaN(doDays) ? doDays : 0}` : `0`, //Jumlah Selisih Hari DO-PO Eksternal
                            deliveryOrderDaysRange: poFulfillment.deliveryOrderDate ? `'${getRangeMonth(doDays)}'` : null, //Selisih Hari DO-PO Eksternal
                            supplierCode: (poItem.purchaseOrderExternal && poItem.supplier && poItem.supplier.code !== undefined) ? `'${poItem.supplier.code.replace(/'/g, '"')}'` : null, //Kode Supplier
                            supplierName: (poItem.purchaseOrderExternal && poItem.supplier && poItem.supplier.name !== undefined) ? `'${poItem.supplier.name.replace(/'/g, '"')}'` : null, //Nama Supplier
                            currencyCode: (poItem.purchaseOrderExternal && poItem.currency && poItem.currency.code !== undefined) ? `'${poItem.currency.code.replace(/'/g, '"')}'` : null, //Kode Mata Uang
                            currencySymbol: (poItem.purchaseOrderExternal && poItem.currency && poItem.currency.symbol !== undefined) ? `'${poItem.currency.symbol.replace(/'/g, '"')}'` : null, //Simbol Mata Uang
                            paymentMethod: (poItem.purchaseOrderExternal && poItem.paymentMethod !== undefined) ? `'${poItem.paymentMethod.replace(/'/g, '"')}'` : null, //Metode Pembayaran
                            currencyRate: (poItem.purchaseOrderExternal && poItem.currency.rate) ? `${poItem.currency.rate}` : null, //Nilai Mata Uang
                            purchaseQuantity: poItem.defaultQuantity ? `${poItem.defaultQuantity}` : null, //Jumlah Barang
                            uom: (poItem.defaultUom && poItem.defaultUom.unit) ? `'${poItem.defaultUom.unit.replace(/'/g, '"')}'` : null, //UOM
                            pricePerUnit: (poItem.purchaseOrderExternal && poItem.purchaseOrderExternal.no) ? `${poItem.pricePerDealUnit}` : null, //Harga Per Unit
                            totalPrice: (poItem.currency.rate && poItem.pricePerDealUnit && poItem.dealQuantity) ? `${poItem.dealQuantity * poItem.pricePerDealUnit * poItem.currency.rate}` : null, //Total Harga
                            expectedDeliveryDate: (poItem.purchaseOrderExternal && poItem.purchaseOrderExternal.expectedDeliveryDate) ? `'${moment(validateDate(poItem.purchaseOrderExternal.expectedDeliveryDate)).add(7, "h").format('YYYY-MM-DD')}'` : null, //Tanggal Rencana Kedatangan
                            prNoAtPoExt: purchaseRequest.no ? `'${purchaseRequest.no.replace(/'/g, '"')}'` : null, //Nomor PR di PO Eksternal

                            deliveryOrderNo: poFulfillment.deliveryOrderNo ? `'${poFulfillment.deliveryOrderNo.replace(/'/g, '"')}'` : null, //Nomor Surat Jalan (Delivery Order)
                            deliveryOrderDate: poFulfillment.deliveryOrderDate ? `'${moment(validateDate(poFulfillment.deliveryOrderDate)).add(7, "h").format('YYYY-MM-DD')}'` : null, //Tanggal Surat Jalan
                            unitReceiptNoteDays: poFulfillment.unitReceiptNoteDate ? `${!isNaN(urnDays) ? urnDays : 0}` : `0`, //Jumlah Selisih Hari URN-DO
                            unitReceiptNoteDaysRange: poFulfillment.unitReceiptNoteDate ? `'${getRangeWeek(urnDays)}'` : null, //Selisih Hari URN-DO
                            status: poFulfillment.deliveryOrderDate ? `'${getStatus(poItem.purchaseOrderExternal.expectedDeliveryDate, lastDeliveredDate)}'` : null, //Status Ketepatan Waktu
                            prNoAtDo: purchaseRequest.no ? `'${purchaseRequest.no}'` : null, //Nomor PR di DO

                            unitReceiptNoteNo: poFulfillment.unitReceiptNoteNo ? `'${poFulfillment.unitReceiptNoteNo.replace(/'/g, '"')}'` : null, //Nomor Bon Terima Unit (Unit Receipt Note)
                            unitReceiptNoteDate: poFulfillment.unitReceiptNoteDate ? `'${moment(validateDate(poFulfillment.unitReceiptNoteDate)).add(7, "h").format('YYYY-MM-DD')}'` : null, //Tanggal URN
                            unitPaymentOrderDays: poFulfillment.interNoteDate ? `${!isNaN(upoDays) ? upoDays : 0}` : `0`, //Jumlah Selisih Hari UPO-URN
                            unitPaymentOrderDaysRange: poFulfillment.interNoteDate ? `'${getRangeWeek(upoDays)}'` : null, //Selisih Hari UPO-URN

                            unitPaymentOrderNo: poFulfillment.interNoteNo ? `'${poFulfillment.interNoteNo.replace(/'/g, '"')}'` : null, //Nomor Surat Perintah Bayar
                            unitPaymentOrderDate: poFulfillment.interNoteDate ? `'${moment(validateDate(poFulfillment.interNoteDate)).add(7, "h").format('YYYY-MM-DD')}'` : null, //Tanggal SPB
                            purchaseOrderDays: poFulfillment.interNoteDate ? `${!isNaN(poDays) ? poDays : 0}` : `0`, //Jumlah Selisih Hari UPO-PO Internal
                            purchaseOrderDaysRange: poFulfillment.interNoteDate ? `'${getRangeMonth(poDays)}'` : null, //Selisih Hari UPO-PO Internal
                            invoicePrice: poFulfillment.interNotePrice ? `'${poFulfillment.interNotePrice}'` : null, //Harga Sesuai Invoice
                            unitPaymentOrderPrice: poFulfillment.interNotePrice ? `'${poFulfillment.interNotePrice}'` : null,
                            unitPaymentOrderQuantity: poFulfillment.interNoteQuantity ? `'${poFulfillment.interNoteQuantity}'` : null,
                            unitPaymentOrderDueDate: poFulfillment.interNoteDueDate ? `'${moment(validateDate(poFulfillment.interNoteDueDate)).add(7, "h").format('YYYY-MM-DD')}'` : null,
                            unitReceiptNoteDeliveredQuantity: poFulfillment.unitReceiptNoteDeliveredQuantity != undefined ? `'${poFulfillment.unitReceiptNoteDeliveredQuantity}'` : null
                        };
                    });
                } else if (!poItem.fulfillments || poItem.fulfillments.length === 0) {
                    var prPoExtDays = (poItem.purchaseOrderExternal && poItem.purchaseOrderExternal._createdDate) ? moment(moment(validateDate(poItem.purchaseOrderExternal._createdDate)).add(7, "h").startOf("day")).diff(moment(moment(validateDate(purchaseRequest._createdDate)).add(7, "h").startOf("day")), "days") : null;
                    var poExtDays = (poItem.purchaseOrderExternal && poItem.purchaseOrderExternal._createdDate) ? moment(moment(validateDate(poItem.purchaseOrderExternal._createdDate)).add(7, "h").startOf("day")).diff(moment(moment(validateDate(purchaseOrder._createdDate)).add(7, "h").startOf("day")), "days") : null;
                    var poIntDays = purchaseOrder._createdDate ? moment(moment(validateDate(purchaseOrder._createdDate)).add(7, "h").startOf("day")).diff(moment(moment(validateDate(purchaseRequest._createdDate)).add(7, "h").startOf("day")), "days") : null;

                    // if (poItem.purchaseOrderExternal && poItem.supplier && poItem.supplier.code == undefined) {
                    //     console.log()
                    // }
                    return {
                        purchaseRequestNo: purchaseRequest.no ? `'${purchaseRequest.no.replace(/'/g, '"')}'` : null, //Nomor PR
                        purchaseRequestDate: purchaseRequest._createdDate ? `'${moment(validateDate(purchaseRequest._createdDate)).add(7, "h").format('YYYY-MM-DD')}'` : null, //Tanggal PR
                        expectedPRDeliveryDate: purchaseRequest.expectedDeliveryDate ? `'${moment(validateDate(purchaseRequest.expectedDeliveryDate)).add(7, "h").format('YYYY-MM-DD')}'` : null, //Tanggal Diminta Datang
                        unitCode: (purchaseRequest.unit && purchaseRequest.unit.code) ? `'${purchaseRequest.unit.code.replace(/'/g, '"')}'` : null, //Kode Unit
                        unitName: (purchaseRequest.unit && purchaseRequest.unit.name) ? `'${purchaseRequest.unit.name.replace(/'/g, '"')}'` : null, //Nama Unit
                        divisionCode: (purchaseRequest.unit && purchaseRequest.unit.division && purchaseRequest.unit.division.code) ? `'${purchaseRequest.unit.division.code.replace(/'/g, '"')}'` : null, //Kode Divisi
                        divisionName: (purchaseRequest.unit && purchaseRequest.unit.division && purchaseRequest.unit.division.name) ? `'${purchaseRequest.unit.division.name.replace(/'/g, '"')}'` : null, //Nama Divisi
                        categoryCode: (poItem.category && poItem.category.code) ? `'${poItem.category.code.replace(/'/g, '"')}'` : null, //Kode Kategori
                        categoryName: (poItem.category && poItem.category.name) ? `'${poItem.category.name.replace(/'/g, '"')}'` : null, //Nama Kategori
                        categoryType: (poItem.category && poItem.category.code) ? `'${getCategoryType(poItem.category.code.replace(/'/g, '"'))}'` : null, //Jenis Kategori
                        productCode: (poItem.product && poItem.product.code) ? `'${poItem.product.code.replace(/'/g, '"')}'` : null, //Kode Produk
                        productName: (poItem.product && poItem.product.name) ? `'${poItem.product.name.replace(/'/g, '"')}'` : null, //Nama Produk
                        purchaseRequestDays: `${!isNaN(poIntDays) ? poIntDays : 0}`, //Jumlah Selisih Hari PR-PO Internal
                        purchaseRequestDaysRange: poIntDays !== null ? `'${getRangeWeek(poIntDays)}'` : null, //Selisih Hari PR-PO Internal
                        prPurchaseOrderExternalDays: `${!isNaN(prPoExtDays) ? prPoExtDays : 0}`, //Jumlah Selisih Hari PR-PO Eksternal
                        prPurchaseOrderExternalDaysRange: prPoExtDays !== null ? `'${getRangeWeek(prPoExtDays)}'` : null, //Selisih Hari PR-PO Eksternal
                        deletedPR: `'${purchaseRequest._deleted}'`,

                        purchaseOrderNo: purchaseOrder.no ? `'${purchaseOrder.no.replace(/'/g, '"')}'` : null, //Nomor PO Internal
                        purchaseOrderDate: purchaseOrder._createdDate ? `'${moment(validateDate(purchaseOrder._createdDate)).add(7, "h").format('YYYY-MM-DD')}'` : null, //Tanggal PO Internal
                        purchaseOrderExternalDays: `${!isNaN(poExtDays) ? poExtDays : 0}`, //Jumlah Selisih Hari PO Internal-PO Eksternal
                        purchaseOrderExternalDaysRange: poExtDays !== null ? `'${getRangeWeek(poExtDays)}'` : null, //Selisih Hari PO Internal-PO Eksternal
                        purchasingStaffName: purchaseOrder._createdBy ? `'${purchaseOrder._createdBy.replace(/'/g, '"')}'` : null, //Nama Staff Pembelian
                        prNoAtPo: purchaseRequest.no ? `'${purchaseRequest.no.replace(/'/g, '"')}'` : null, //Nomor PR di PO Internal
                        deletedPO: `'${purchaseOrder._deleted}'`,

                        purchaseOrderExternalNo: (poItem.purchaseOrderExternal && poItem.purchaseOrderExternal.no) ? `'${poItem.purchaseOrderExternal.no}'` : null, // Nomor PO Eksternal
                        purchaseOrderExternalDate: (poItem.purchaseOrderExternal && poItem.purchaseOrderExternal._createdDate) ? `'${moment(validateDate(poItem.purchaseOrderExternal._createdDate)).add(7, "h").format('YYYY-MM-DD')}'` : null, //Tanggal PO Eksternal
                        deliveryOrderDays: null, //Jumlah Selisih Hari DO-PO Eksternal
                        deliveryOrderDaysRange: null, //Selisih Hari DO-PO Eksternal
                        supplierCode: (poItem.purchaseOrderExternal && poItem.supplier && poItem.supplier.code !== undefined) ? `'${poItem.supplier.code.replace(/'/g, '"')}'` : null, //Kode Supplier
                        supplierName: (poItem.purchaseOrderExternal && poItem.supplier && poItem.supplier.name !== undefined) ? `'${poItem.supplier.name.replace(/'/g, '"')}'` : null, //Nama Supplier
                        currencyCode: (poItem.purchaseOrderExternal && poItem.currency && poItem.currency.code !== undefined) ? `'${poItem.currency.code.replace(/'/g, '"')}'` : null, //Kode Mata Uang
                        currencySymbol: (poItem.purchaseOrderExternal && poItem.currency && poItem.currency.symbol !== undefined) ? `'${poItem.currency.symbol.replace(/'/g, '"')}'` : null, //Simbol Mata Uang
                        paymentMethod: (poItem.purchaseOrderExternal && poItem.paymentMethod !== undefined) ? `'${poItem.paymentMethod.replace(/'/g, '"')}'` : null, //Metode Pembayaran
                        currencyRate: (poItem.purchaseOrderExternal && poItem.currency.rate) ? `${poItem.currency.rate}` : null, //Nilai Mata Uang
                        purchaseQuantity: poItem.defaultQuantity ? `${poItem.defaultQuantity}` : null, //Jumlah Barang
                        uom: (poItem.defaultUom && poItem.defaultUom.unit) ? `'${poItem.defaultUom.unit.replace(/'/g, '"')}'` : null, //UOM
                        pricePerUnit: (poItem.purchaseOrderExternal && poItem.purchaseOrderExternal.no) ? `${poItem.pricePerDealUnit}` : null, //Harga Per Unit
                        totalPrice: (poItem.currency && poItem.currency.rate && poItem.pricePerDealUnit && poItem.dealQuantity) ? `${poItem.dealQuantity * poItem.pricePerDealUnit * poItem.currency.rate}` : null, //Total Harga
                        expectedDeliveryDate: (poItem.purchaseOrderExternal && poItem.purchaseOrderExternal.expectedDeliveryDate) ? `'${moment(validateDate(poItem.purchaseOrderExternal.expectedDeliveryDate)).add(7, "h").format('YYYY-MM-DD')}'` : null, //Tanggal Rencana Kedatangan
                        prNoAtPoExt: purchaseRequest.no ? `'${purchaseRequest.no}'` : null, //Nomor PR di PO Eksternal

                        deliveryOrderNo: null, //Nomor Surat Jalan (Delivery Order)
                        deliveryOrderDate: null, //Tanggal Surat Jalan
                        unitReceiptNoteDays: null, //Jumlah Selisih Hari URN-DO
                        unitReceiptNoteDaysRange: null, //Selisih Hari URN-DO
                        status: null, //Status Ketepatan Waktu
                        prNoAtDo: null, //Nomor PR di DO

                        unitReceiptNoteNo: null, //Nomor Bon Terima Unit (Unit Receipt Note)
                        unitReceiptNoteDate: null, //Tanggal URN
                        unitPaymentOrderDays: null, //Jumlah Selisih Hari UPO-URN
                        unitPaymentOrderDaysRange: null, //Selisih Hari UPO-URN

                        unitPaymentOrderNo: null, //Nomor Surat Perintah Bayar
                        unitPaymentOrderDate: null, //Tanggal SPB
                        purchaseOrderDays: null, //Jumlah Selisih Hari UPO-PO Internal
                        purchaseOrderDaysRange: null, //Selisih Hari UPO-PO Internal
                        invoicePrice: null, //Harga Sesuai Invoice,
                        unitPaymentOrderPrice: null,
                        unitPaymentOrderQuantity: null,
                        unitPaymentOrderDueDate: null,
                        unitReceiptNoteDeliveredQuantity: null
                    }
                }
            });
            return [].concat.apply([], results);
        }
        else if (purchaseRequest) {
            var results = purchaseRequest.items.map((poItem) => {

                return {
                    purchaseRequestNo: purchaseRequest.no ? `'${purchaseRequest.no.replace(/'/g, '"')}'` : null, //Nomor PR
                    purchaseRequestDate: purchaseRequest._createdDate ? `'${moment(validateDate(purchaseRequest._createdDate)).add(7, "h").format('YYYY-MM-DD')}'` : null, //Tanggal PR
                    expectedPRDeliveryDate: purchaseRequest.expectedDeliveryDate ? `'${moment(validateDate(purchaseRequest.expectedDeliveryDate)).add(7, "h").format('YYYY-MM-DD')}'` : null, //Tanggal Diminta Datang
                    unitCode: (purchaseRequest.unit && purchaseRequest.unit.code) ? `'${purchaseRequest.unit.code.replace(/'/g, '"')}'` : null, //Kode Unit
                    unitName: (purchaseRequest.unit && purchaseRequest.unit.name) ? `'${purchaseRequest.unit.name.replace(/'/g, '"')}'` : null, //Nama Unit
                    divisionCode: (purchaseRequest.unit && purchaseRequest.unit.division && purchaseRequest.unit.division.code) ? `'${purchaseRequest.unit.division.code.replace(/'/g, '"')}'` : null, //Kode Divisi
                    divisionName: (purchaseRequest.unit && purchaseRequest.unit.division && purchaseRequest.unit.division.name) ? `'${purchaseRequest.unit.division.name.replace(/'/g, '"')}'` : null, //Nama Divisi
                    categoryCode: (poItem.category && poItem.category.code) ? `'${poItem.category.code.replace(/'/g, '"')}'` : null, //Kode Kategori
                    categoryName: (poItem.category && poItem.category.name) ? `'${poItem.category.name.replace(/'/g, '"')}'` : null, //Nama Kategori
                    categoryType: (poItem.category && poItem.category.code) ? `'${getCategoryType(poItem.category.code.replace(/'/g, '"'))}'` : null, //Jenis Kategori
                    productCode: (poItem.product && poItem.product.code) ? `'${poItem.product.code.replace(/'/g, '"')}'` : null, //Kode Produk
                    productName: (poItem.product && poItem.product.name) ? `'${poItem.product.name.replace(/'/g, '"')}'` : null, //Nama Produk
                    purchaseRequestDays: null, //Jumlah Selisih Hari PR-PO Internal
                    purchaseRequestDaysRange: null, //Selisih Hari PR-PO Internal
                    prPurchaseOrderExternalDays: null, //Jumlah Selisih Hari PR-PO Eksternal
                    prPurchaseOrderExternalDaysRange: null, //Selisih Hari PR-PO Eksternal
                    deletedPR: `'${purchaseRequest._deleted}'`,

                    purchaseOrderNo: null, //Nomor PO Internal
                    purchaseOrderDate: null, //Tanggal PO Internal
                    purchaseOrderExternalDays: null, //Jumlah Selisih Hari PO Internal-PO Eksternal
                    purchaseOrderExternalDaysRange: null, //Selisih Hari PO Internal-PO Eksternal
                    purchasingStaffName: purchaseRequest._createdBy ? `'${purchaseRequest._createdBy.replace(/'/g, '"')}'` : null, //Nama Staff Pembelian
                    prNoAtPo: null, //Nomor PR di PO Internal
                    deletedPO: null,

                    purchaseOrderExternalNo: null, // Nomor PO Eksternal
                    purchaseOrderExternalDate: null, //Tanggal PO Eksternal
                    deliveryOrderDays: null, //Jumlah Selisih Hari DO-PO Eksternal
                    deliveryOrderDaysRange: null, //Selisih Hari DO-PO Eksternal
                    supplierCode: null, //Kode Supplier
                    supplierName: null, //Nama Supplier
                    currencyCode: null, //Kode Mata Uang
                    currencySymbol: null, //Simbol Mata Uang
                    paymentMethod: null, //Metode Pembayaran
                    currencyRate: null, //Nilai Mata Uang
                    purchaseQuantity: poItem.quantity ? `${poItem.quantity}` : null, //Jumlah Barang
                    uom: (poItem.uom && poItem.uom.unit) ? `'${poItem.uom.unit.replace(/'/g, '"')}'` : null, //UOM
                    pricePerUnit: null, //Harga Per Unit
                    totalPrice: null, //Total Harga
                    expectedDeliveryDate: null, //Tanggal Rencana Kedatangan
                    prNoAtPoExt: null, //Nomor PR di PO Eksternal

                    deliveryOrderNo: null, //Nomor Surat Jalan (Delivery Order)
                    deliveryOrderDate: null, //Tanggal Surat Jalan
                    unitReceiptNoteDays: null, //Jumlah Selisih Hari URN-DO
                    unitReceiptNoteDaysRange: null, //Selisih Hari URN-DO
                    status: null, //Status Ketepatan Waktu
                    prNoAtDo: null, //Nomor PR di DO

                    unitReceiptNoteNo: null, //Nomor Bon Terima Unit (Unit Receipt Note)
                    unitReceiptNoteDate: null, //Tanggal URN
                    unitPaymentOrderDays: null, //Jumlah Selisih Hari UPO-URN
                    unitPaymentOrderDaysRange: null, //Selisih Hari UPO-URN

                    unitPaymentOrderNo: null, //Nomor Surat Perintah Bayar
                    unitPaymentOrderDate: null, //Tanggal SPB
                    purchaseOrderDays: null, //Jumlah Selisih Hari UPO-PO Internal
                    purchaseOrderDaysRange: null, //Selisih Hari UPO-PO Internal
                    invoicePrice: null, //Harga Sesuai Invoice
                    unitPaymentOrderPrice: null,
                    unitPaymentOrderQuantity: null,
                    unitPaymentOrderDueDate: null,
                    unitReceiptNoteDeliveredQuantity: null
                };
            });
            return [].concat.apply([], results);
        }
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
                var sqlQuery = 'INSERT INTO [DL_Fact_Pembelian_Garment_Temp] ';
                var count = 1;
                for (var item of data) {
                    if (item) {
                        var queryString = `\nSELECT ${item.purchaseRequestNo}, ${item.purchaseRequestDate}, ${item.expectedPRDeliveryDate}, ${item.unitCode}, ${item.unitName}, ${item.divisionCode}, ${item.divisionName}, ${item.categoryCode}, ${item.categoryName}, ${item.categoryType}, ${item.productCode}, ${item.productName}, ${item.purchaseRequestDays}, ${item.purchaseRequestDaysRange}, ${item.prPurchaseOrderExternalDays}, ${item.prPurchaseOrderExternalDaysRange}, ${item.deletedPR}, ${item.purchaseOrderNo}, ${item.purchaseOrderDate}, ${item.purchaseOrderExternalDays}, ${item.purchaseOrderExternalDaysRange}, ${item.purchasingStaffName}, ${item.prNoAtPo}, ${item.deletedPO}, ${item.purchaseOrderExternalNo}, ${item.purchaseOrderExternalDate}, ${item.deliveryOrderDays}, ${item.deliveryOrderDaysRange}, ${item.supplierCode}, ${item.supplierName}, ${item.currencyCode}, ${item.currencySymbol}, ${item.paymentMethod}, ${item.currencyRate}, ${item.purchaseQuantity}, ${item.uom}, ${item.pricePerUnit}, ${item.totalPrice}, ${item.expectedDeliveryDate}, ${item.prNoAtPoExt}, ${item.deliveryOrderNo}, ${item.deliveryOrderDate}, ${item.unitReceiptNoteDays}, ${item.unitReceiptNoteDaysRange}, ${item.status}, ${item.prNoAtDo}, ${item.unitReceiptNoteNo}, ${item.unitReceiptNoteDate}, ${item.unitPaymentOrderDays}, ${item.unitPaymentOrderDaysRange},${item.unitPaymentOrderNo}, ${item.unitPaymentOrderDate}, ${item.purchaseOrderDays}, ${item.purchaseOrderDaysRange}, ${item.invoicePrice}, ${item.unitPaymentOrderPrice}, ${item.unitPaymentOrderQuantity}, ${item.unitPaymentOrderDueDate}, ${item.unitReceiptNoteDeliveredQuantity} UNION ALL `;
                        sqlQuery = sqlQuery.concat(queryString);
                        if (count % 200 == 0) {
                            sqlQuery = sqlQuery.substring(0, sqlQuery.length - 10);
                            command.push(insertQuery(sqlDWHConnections.sqlDWH, sqlQuery, t));
                            sqlQuery = 'INSERT INTO [DL_Fact_Pembelian_Garment_Temp] ';
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
                        sqlDWHConnections.sqlDWH.query("exec [DL_UPSERT_FACT_GARMENT_PEMBELIAN]", {
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

