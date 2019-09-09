let sqlDWHConnections = require('../Connection/DWH/');
let sqlFPConnection = require('../Connection/FinishingPrinting/');
let sqlSalesConnection = require('../Connection/Sales/');
let sqlCoreConnection = require('../Connection/Core/');
let sqlPurchasingConnection = require('../Connection/Purchasing');
const minimumDateString = "1753-01-01";
let moment = require('moment');

module.exports = async function () {
    return await joinPembelianCurrency()
        .then((data) => transform(data))
        .then((data) => load(data));

}

const getRangeMonth = function (days) {
    if (days <= 30) {
        return "0-30 hari";
    } else if (days >= 31 && days <= 60) {
        return "31-60 hari";
    } else if (days >= 61 && days <= 90) {
        return "61-90 hari";
    } else if (days > 90) {
        return ">90 hari";
    }
};

const getRangeWeek = function (days) {
    if (days <= 7) {
        return "0-7 hari";
    } else if (days >= 8 && days <= 14) {
        return "8-14 hari";
    } else if (days >= 15 && days <= 30) {
        return "15-30 hari";
    } else if (days > 30) {
        return ">30 hari";
    }
};

const getCategoryType = function (catType) {
    if (catType === "BAHAN BAKU") {
        return "BAHAN BAKU";
    } else {
        return "NON BAHAN BAKU";
    }
}

const getStatus = function (poDate, doDate) {
    var poDates = moment(poDate).startOf("day");
    var doDates = moment(doDate).startOf("day");
    var result = moment(doDates).diff(moment(poDates), "days")
    if (result <= 0) {
        return "Tepat Waktu";
    } else {
        return "Tidak Tepat Waktu";
    }
}

const validateDate = function (date) {
    let minimumDate = new Date(minimumDateString);
    let dateToValidate = new Date(date);
    let dateNow = new Date();

    if (dateToValidate < minimumDate) {
        dateToValidate = new Date(dateToValidate.setFullYear(dateNow.getFullYear()))
    }

    return dateToValidate;
}

const extractPembelian = function () {
    return sqlPurchasingConnection
        .sqlPURCHASING
        .query(`SELECT
        pr.categoryName,
        pr.createdUtc prCreatedUtc,
        pr.no prNo,
        pr.expectedDeliveryDate prExpectedDeliveryDate,
        pr.budgetCode,
        pr.budgetName,
        pr.unitCode,
        pr.unitName,
        pr.divisionCode,
        pr.divisionName,
        pr.categoryCode,
        pr.isDeleted prDeleted,
        pr.id prId,
        ipo.createdBy ipoCreatedBy,
        ipo.CreatedUtc ipoCreatedUtc,
        ipo.PONo,
        ipo.IsDeleted ipoIsDeleted,
        ipo.prNo ipoPrNo,
        ipo.id ipoId,
        e.createdUtc epoCreatedUtc,
        e.epoNo,
        e.supplierCode,
        e.supplierName,
        e.currencyId,
        e.currencyCode,
        e.paymentMethod,
        e.currencyRate,
        ipo.expectedDeliveryDate ipoExpectedDeliveryDate,
        ed.productCode,
        ed.productName,
        ed.dealQuantity,
        ed.dealUomUnit,
        ed.pricePerDealUnit,
        d.DONo,
        d.DODate,
        urn.URNNo,
        urn.receiptDate,
        upo.UPONo interNoteNo,
        upo.Date interNoteDate
        FROM purchaserequests pr left join InternalPurchaseOrders ipo on pr.Id = ipo.PRId left join  ExternalPurchaseOrderItems ei on ipo.Id = ei.poId left join ExternalPurchaseOrderDetails ed on ei.Id = ed.EPOItemId left join ExternalPurchaseOrders e on ei.EPOId = e.Id left join DeliveryOrderItems doi on e.Id = doi.EPOId left join DeliveryOrders d on doi.DOId = d.Id left join UnitReceiptNotes urn on d.Id = urn.doId left join UnitPaymentOrderItems upoi on urn.id = upoi.URNId left join UnitPaymentOrders upo on upoi.UPOId = upo.Id`, {
            type: sqlPurchasingConnection.sqlPURCHASING.QueryTypes.SELECT
        });
};

const getCurrency = function () {
    return sqlCoreConnection
        .sqlCore
        .query(`SELECT  
        id,
        description
        FROM currencies `, {
            type: sqlCoreConnection.sqlCore.QueryTypes.SELECT
        });
}

const joinPembelianCurrency = function () {
    var pembelian = extractPembelian();
    var currencies = getCurrency();
    return Promise.all([pembelian, currencies])
        .then((data) => {
            var pembelian = data[0];
            var currencies = data[1];

            for (var item of pembelian) {
                var currency = currencies.find(x => x.id == item.currencyId);
                item.currencyDescription = currency ? currency.description : null;
                item.currencyName = item.currencyDescription;
            }

            return Promise.resolve(pembelian);
        });
}

const transform = function (data) {

    var result = data.map((item) => {
        var catType = (item.categoryName) ? item.categoryName : null;

        var prPoExtDays = (item.epoCreatedUtc) ? moment(moment(validateDate(item.epoCreatedUtc)).startOf("day")).diff(moment(moment(validateDate(item.prCreatedUtc)).startOf("day")), "days") : null;
        var poIntDays = item.ipoCreatedUtc ? moment(moment(validateDate(item.ipoCreatedUtc)).startOf("day")).diff(moment(moment(validateDate(item.prCreatedUtc)).startOf("day")), "days") : null;
        var poExtDays = (item.epoCreatedUtc) ? moment(moment(validateDate(item.epoCreatedUtc)).startOf("day")).diff(moment(moment(validateDate(item.ipoCreatedUtc)).startOf("day")), "days") : null;


        var doDays = (item.DODate && item.epoCreatedUtc) ? moment(moment(validateDate(item.DODate)).startOf("day")).diff(moment(moment(validateDate(item.epoCreatedUtc)).startOf("day")), "days") : null;
        var urnDays = item.receiptDate ? moment(moment(validateDate(item.receiptDate)).startOf("day")).diff(moment(moment(validateDate(item.DODate)).startOf("day")), "days") : null;
        var upoDays = item.interNoteDate ? moment(moment(validateDate(item.interNoteDate)).startOf("day")).diff(moment(moment(validateDate(item.receiptDate)).startOf("day")), "days") : null;
        var poDays = item.interNoteDate ? moment(moment(validateDate(item.interNoteDate)).startOf("day")).diff(moment(moment(validateDate(item.ipoCreatedUtc)).startOf("day")), "days") : null;
        var lastDeliveredDate = item.DODate ? item.DODate : null;
        return {
            purchaseRequestNo: item.prNo ? `'${item.prNo}'` : null,
            purchaseRequestDate: item.prCreatedUtc ? `'${moment(validateDate(item.prCreatedUtc)).add(7, "hours").format('YYYY-MM-DD')}'` : null,
            expectedPRDeliveryDate: item.prExpectedDeliveryDate ? `'${moment(validateDate(item.prExpectedDeliveryDate)).add(7, "hours").format('YYYY-MM-DD')}'` : null,
            budgetCode: (item.budgetCode) ? `'${item.budgetCode}'` : null,
            budgetName: (item.budgetName) ? `'${item.budgetName}'` : null,
            unitCode: (item.unitCode) ? `'${item.unitCode}'` : null,
            unitName: (item.unitName) ? `'${item.unitName}'` : null,
            divisionCode: (item.divisionCode) ? `'${item.divisionCode}'` : null,
            divisionName: (item.divisionName) ? `'${item.divisionName}'` : null,
            categoryCode: (item.categoryCode) ? `'${item.categoryCode}'` : null,
            categoryName: (item.categoryName) ? `'${item.categoryName}'` : null,
            categoryType: (item.categoryName) ? `'${getCategoryType(catType)}'` : null,
            productCode: (item.productCode) ? `'${item.productCode}'` : null,
            productName: (item.productName) ? `'${item.productName.replace(/'/g, '"')}'` : null,
            purchaseRequestDays: item.ipoCreatedUtc ? `${poIntDays}` : null,
            purchaseRequestDaysRange: item.ipoCreatedUtc ? `'${getRangeWeek(poIntDays)}'` : null,
            prPurchaseOrderExternalDays: (item.epoCreatedUtc) ? `${prPoExtDays}` : null,
            prPurchaseOrderExternalDaysRange: (item.epoCreatedUtc) ? `'${getRangeWeek(prPoExtDays)}'` : null,

            purchaseOrderNo: item.PONo ? `'${item.PONo}'` : null,
            purchaseOrderDate: item.ipoCreatedUtc ? `'${moment(validateDate(item.ipoCreatedUtc)).add(7, "hours").format('YYYY-MM-DD')}'` : null,
            purchaseOrderExternalDays: (item.epoCreatedUtc) ? `${poExtDays}` : null,
            purchaseOrderExternalDaysRange: (item.epoCreatedUtc) ? `'${getRangeWeek(poExtDays)}'` : null,
            purchasingStaffName: item.ipoCreatedUtc ? `'${item.ipoCreatedBy}'` : null,
            prNoAtPo: item.ipoPrNo ? `'${item.ipoPrNo}'` : null,

            purchaseOrderExternalNo: (item.epoNo) ? `'${item.epoNo}'` : null,
            purchaseOrderExternalDate: (item.epoCreatedUtc) ? `'${moment(validateDate(item.epoCreatedUtc)).add(7, "hours").format('YYYY-MM-DD')}'` : null,
            deliveryOrderDays: (item.epoCreatedUtc && item.DODate) ? `${doDays}` : null,
            deliveryOrderDaysRange: (item.epoCreatedUtc && item.DODate) ? `'${getRangeMonth(doDays)}'` : null,
            supplierCode: (item.supplierCode) ? `'${item.supplierCode}'` : null,
            supplierName: (item.supplierName) ? `'${item.supplierName.replace(/'/g, '"')}'` : null,
            currencyCode: (item.currencyCode) ? `'${item.currencyCode}'` : null,
            currencyName: (item.currencyDescription) ? `'${item.currencyDescription}'` : null,
            paymentMethod: (item.paymentMethod) ? `'${item.paymentMethod}'` : null,
            currencyRate: (item.currencyRate) ? `${item.currencyRate}` : null,
            purchaseQuantity: item.dealQuantity ? `${item.dealQuantity}` : null,
            uom: (item.dealUomUnit) ? `'${item.dealUomUnit}'` : null,
            pricePerUnit: item.pricePerDealUnit ? `${item.pricePerDealUnit}` : null,
            totalPrice: (item.pricePerDealUnit && item.dealQuantity) ? `${item.dealQuantity * item.pricePerDealUnit * item.currencyRate}` : null,
            expectedDeliveryDate: (item.ipoExpectedDeliveryDate) ? `'${moment(validateDate(item.ipoExpectedDeliveryDate)).add(7, "hours").format('YYYY-MM-DD')}'` : null,
            prNoAtPoExt: (item.epoNo) ? `'${item.prNo}'` : null,

            deliveryOrderNo: item.DONo ? `'${item.DONo}'` : null,
            deliveryOrderDate: item.DODate ? `'${moment(validateDate(item.DODate)).add(7, "hours").format('YYYY-MM-DD')}'` : null,
            unitReceiptNoteDays: item.receiptDate ? `${urnDays}` : null,
            unitReceiptNoteDaysRange: item.receiptDate ? `'${getRangeWeek(urnDays)}'` : null,
            status: item.DODate ? `'${getStatus(item.ipoExpectedDeliveryDate, lastDeliveredDate)}'` : null,
            prNoAtDo: item.DONo ? `'${item.prNo}'` : null,

            unitReceiptNoteNo: item.URNNo ? `'${item.URNNo}'` : null,
            unitReceiptNoteDate: item.receiptDate ? `'${moment(validateDate(item.receiptDate)).add(7, "hours").format('YYYY-MM-DD')}'` : null,
            unitPaymentOrderDays: item.interNoteDate ? `${upoDays}` : null,
            unitPaymentOrderDaysRange: item.interNoteDate ? `'${getRangeWeek(upoDays)}'` : null,

            unitPaymentOrderNo: item.interNoteNo ? `'${item.interNoteNo}'` : null,
            unitPaymentOrderDate: item.interNoteDate ? `'${moment(validateDate(item.interNoteDate)).add(7, "hours").format('YYYY-MM-DD')}'` : null,
            purchaseOrderDays: item.interNoteDate ? `${poDays}` : null,
            purchaseOrderDaysRange: item.interNoteDate ? `'${getRangeMonth(poDays)}'` : null,
            invoicePrice: item.interNoteDate ? `'${item.pricePerDealUnit}'` : null,
            deletedPR: `'${item.prDeleted}'`,
            deletedPO: `'${item.ipoIsDeleted}'`
        };
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
                var sqlQuery = '';

                var count = 1;
                for (var item of data) {
                    if (item) {
                        var queryString = `INSERT INTO dl_fact_pembelian_temp([Nomor PR], [Tanggal PR], [Tanggal Diminta Datang], [Kode Budget], [Nama Budget], [Kode Unit], [Nama Unit], [Kode Divisi], [Nama Divisi], [Kode Kategori], [Nama Kategori], [Jenis Kategori], [Kode Produk], [Nama Produk], [Jumlah Selisih Hari PR-PO Internal], [Selisih Hari PR-PO Internal], [Jumlah Selisih Hari PR-PO Eksternal], [Selisih Hari PR-PO Eksternal], [Nomor PO Internal], [Tanggal PO Internal], [Jumlah Selisih Hari PO Eksternal-PO Internal], [Selisih Hari PO Eksternal-PO Internal], [Nama Staff Pembelian], [Nomor PR di PO Internal], [Nomor PO Eksternal], [Tanggal PO Eksternal], [Jumlah Selisih Hari DO-PO Eksternal], [Selisih Hari DO-PO Eksternal], [Kode Supplier], [Nama Supplier], [Kode Mata Uang], [Nama Mata Uang], [Metode Pembayaran], [Nilai Mata Uang], [Jumlah Barang], [UOM], [Harga Per Unit], [Total Harga], [Tanggal Rencana Kedatangan], [Nomor PR di PO Eksternal], [Nomor DO], [Tanggal DO], [Jumlah Selisih Hari URN-DO], [Selisih Hari URN-DO], [Status Ketepatan Waktu], [Nomor PR di DO], [Nomor URN], [Tanggal URN], [Jumlah Selisih Hari UPO-URN], [Selisih Hari UPO-URN], [Nomor UPO], [Tanggal UPO], [Jumlah Selisih Hari UPO-PO Internal], [Selisih Hari UPO-PO Internal], [Harga Sesuai Invoice], [deleted PR], [deleted PO]) VALUES(${item.purchaseRequestNo}, ${item.purchaseRequestDate === null ? null : item.purchaseRequestDate.replace("/0017", "/2017").replace("/12017", "/2017").replace("/0200", "/2017").replace("/0201", "/2017").replace("/42017", "/2017").replace("/0217", "/2017")}, ${item.expectedPRDeliveryDate === null ? null : item.expectedPRDeliveryDate.replace("/0017", "/2017").replace("/12017", "/2017").replace("/0200", "/2017").replace("/0201", "/2017").replace("/42017", "/2017").replace("/0217", "/2017")}, ${item.budgetCode}, ${item.budgetName}, ${item.unitCode}, ${item.unitName}, ${item.divisionCode}, ${item.divisionName}, ${item.categoryCode}, ${item.categoryName}, ${item.categoryType}, ${item.productCode}, ${item.productName}, ${item.purchaseRequestDays}, ${item.purchaseRequestDaysRange}, ${item.prPurchaseOrderExternalDays}, ${item.prPurchaseOrderExternalDaysRange}, ${item.purchaseOrderNo}, ${item.purchaseOrderDate === null ? null : item.purchaseOrderDate.replace("/0017", "/2017").replace("/12017", "/2017").replace("/0200", "/2017").replace("/0201", "/2017").replace("/42017", "/2017").replace("/0217", "/2017")}, ${item.purchaseOrderExternalDays}, ${item.purchaseOrderExternalDaysRange}, ${item.purchasingStaffName}, ${item.prNoAtPo}, ${item.purchaseOrderExternalNo}, ${item.purchaseOrderExternalDate === null ? null : item.purchaseOrderExternalDate.replace("/0017", "/2017").replace("/12017", "/2017").replace("/0200", "/2017").replace("/0201", "/2017").replace("/42017", "/2017").replace("/0217", "/2017")}, ${item.deliveryOrderDays}, ${item.deliveryOrderDaysRange}, ${item.supplierCode}, ${item.supplierName}, ${item.currencyCode}, ${item.currencyName}, ${item.paymentMethod}, ${item.currencyRate}, ${item.purchaseQuantity}, ${item.uom}, ${item.pricePerUnit}, ${item.totalPrice}, ${item.expectedDeliveryDate === null ? null : item.expectedDeliveryDate.replace("/0017", "/2017").replace("/12017", "/2017").replace("/0200", "/2017").replace("/0201", "/2017").replace("/42017", "/2017").replace("/0217", "/2017")}, ${item.prNoAtPoExt}, ${item.deliveryOrderNo}, ${item.deliveryOrderDate === null ? null : item.deliveryOrderDate.replace("/0017", "/2017").replace("/12017", "/2017").replace("/0200", "/2017").replace("/0201", "/2017").replace("/42017", "/2017").replace("/0217", "/2017")}, ${item.unitReceiptNoteDays}, ${item.unitReceiptNoteDaysRange}, ${item.status}, ${item.prNoAtDo}, ${item.unitReceiptNoteNo}, ${item.unitReceiptNoteDate === null ? null : item.unitReceiptNoteDate.replace("/0017", "/2017").replace("/12017", "/2017").replace("/0200", "/2017").replace("/0201", "/2017").replace("/42017", "/2017").replace("/0217", "/2017")}, ${item.unitPaymentOrderDays}, ${item.unitPaymentOrderDaysRange}, ${item.unitPaymentOrderNo}, ${item.unitPaymentOrderDate === null ? null : item.unitPaymentOrderDate.replace("/0017", "/2017").replace("/12017", "/2017").replace("/0200", "/2017").replace("/0201", "/2017").replace("/42017", "/2017").replace("/0217", "/2017")}, ${item.purchaseOrderDays}, ${item.purchaseOrderDaysRange}, ${item.invoicePrice}, ${item.deletedPR}, ${item.deletedPO});\n`;
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
                        sqlDWHConnections.sqlDWH.query("exec DL_UPSERT_FACT_PEMBELIAN", {
                            transaction: t
                        }).then((execResult) => {
                            sqlDWHConnections.sqlDWH.query("exec DL_INSERT_DIMTIME", {
                                transaction: t
                            })
                                .then((execResult) => {
                                    t.commit()
                                        .then(() => {
                                            resolve(results);
                                        })
                                        .catch((err) => {
                                            reject(err);
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


