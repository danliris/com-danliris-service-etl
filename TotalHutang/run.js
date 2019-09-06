let sqlDWHConnections = require('../Connection/DWH/');
let sqlFPConnection = require('../Connection/FinishingPrinting/');
let sqlSalesConnection = require('../Connection/Sales/');
let sqlPurchasingConnection = require('../Connection/Purchasing');

let moment = require('moment');

module.exports = async function () {
    return await extractURN()
        .then((data) => transform(data))
        .then((data) => load(data));

}

const extractURN = async function () {
    var urns = await sqlPurchasingConnection
        .sqlPURCHASING
        .query(`select top(1)
        id,
        urnNo,
        unitName
         from unitreceiptnotes`, {
            type: sqlPurchasingConnection.sqlPURCHASING.QueryTypes.SELECT
        });

    var results = [];
    for (var element of urns) {
        var item = {};
        item.unitReceiptNote = element;
        item.unitReceiptNote.unit = {};
        item.unitReceiptNote.unit.name = item.unitReceiptNote.unitName;
        item.unitReceiptNote.items = await joinURNItems(element);
        item.unitPaymentOrder = await joinUPO(element);
        results.push(item);
    }

    return results;
};

const joinURNItems = async function (data) {
    var urnItems = await sqlPurchasingConnection
        .sqlPURCHASING
        .query(` select
        urni.pricePerDealUnit,
        urni.receiptQuantity deliveredQuantity,
        ep.currencyRate,
        urni.productName,
        urni.productCode
        from unitreceiptnoteitems  urni left join ExternalPurchaseOrderDetails epid on urni.EPODetailId = epid.Id left join ExternalPurchaseOrderItems epi on epid.EPOItemId = epi.Id left join ExternalPurchaseOrders ep on epi.EPOId = ep.Id
        where urnid = ?`, {
            replacements: [data.id],
            type: sqlPurchasingConnection.sqlPURCHASING.QueryTypes.SELECT
        });

    for (var element of urnItems) {
        element.product = {};
        element.product.name = element.productName;
        element.product.code = element.productCode;
    }

    return urnItems;
};

const joinUPO = async function (data) {
    var upos = await sqlPurchasingConnection
        .sqlPURCHASING
        .query(`select 
        up.upoNo,
        up.createdUtc,
        up.date,
        up.dueDate,
        up.supplierName,
        up.categoryName,
        up.divisionName
        from unitpaymentorders up inner join unitpaymentorderitems upi on up.id = upi.upoid
        where upi.urnid = ?`, {
            replacements: [data.id],
            type: sqlPurchasingConnection.sqlPURCHASING.QueryTypes.SELECT
        });

    var upo = upos[0];
    if (upo) {
        upo.supplier = {};
        upo.supplier.name = upo.supplierName;
        upo.category = {};
        upo.category.name = upo.categoryName;
        upo.division = {};
        upo.division.name = upo.divisionName;
    }


    return upo;
};

const transform = function (data) {
    var result = data.map((item) => {
        var unitPaymentOrder = item.unitPaymentOrder;
        var unitReceiptNote = item.unitReceiptNote;

        if (unitReceiptNote)

            var results = unitReceiptNote.items.map((unitReceiptNoteItem) => {

                return {
                    unitPaymentOrderNo: `'${unitPaymentOrder.upoNo}'`,
                    unitPaymentOrderDate: `'${moment(unitPaymentOrder.date).add(7, "hours").format("YYYY-MM-DD")}'`,
                    unitPaymentOrderDueDate: `'${moment(unitPaymentOrder.dueDate).add(7, "hours").format("YYYY-MM-DD")}'`,
                    supplierName: `'${unitPaymentOrder.supplier.name.replace(/'/g, '"')}'`,
                    categoryName: `'${unitPaymentOrder.category.name}'`,
                    categoryType: `'${unitPaymentOrder.category.name.toLowerCase() === "bahan baku" ? "BAHAN BAKU" : "NON BAHAN BAKU"}'`,
                    divisionName: `'${unitPaymentOrder.division.name}'`,
                    unitName: `'${unitReceiptNote.unit.name}'`,
                    invoicePrice: `${unitReceiptNoteItem.pricePerDealUnit}`,
                    unitReceiptNoteQuantity: `${unitReceiptNoteItem.deliveredQuantity}`,
                    purchaseOrderExternalCurrencyRate: `${unitReceiptNoteItem.currencyRate}`,
                    total: `${unitReceiptNoteItem.pricePerDealUnit * unitReceiptNoteItem.deliveredQuantity * unitReceiptNoteItem.currencyRate}`,
                    unitReceiptNoteNo: `'${unitReceiptNote.urnNo}'`,
                    productName: `'${unitReceiptNoteItem.product.name.replace(/'/g, '"')}'`,
                    productCode: `'${unitReceiptNoteItem.product.code}'`
                };
            });

        return [].concat.apply([], results);
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
                        var queryString = `insert into dl_fact_total_hutang_temp([ID Fact Total Hutang], [Nomor Nota Intern], [Tanggal Nota Intern], [Nama Supplier], [Jenis Kategori], [Harga Sesuai Invoice], [Jumlah Sesuai Bon Unit], [Rate Yang Disepakati], [Total Harga Nota Intern], [Nama Kategori], [Nama Divisi], [Nama Unit], [nomor bon unit], [nama produk], [kode produk]) values(${count}, ${item.unitPaymentOrderNo}, ${item.unitPaymentOrderDate}, ${item.supplierName}, ${item.categoryType}, ${item.invoicePrice}, ${item.unitReceiptNoteQuantity}, ${item.purchaseOrderExternalCurrencyRate}, ${item.total}, ${item.categoryName}, ${item.divisionName}, ${item.unitName}, ${item.unitReceiptNoteNo}, ${item.productName}, ${item.productCode});\n`;
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
                        sqlDWHConnections.sqlDWH.query("exec DL_UPSERT_FACT_TOTAL_HUTANG", {
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
