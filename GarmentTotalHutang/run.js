let sqlDWHConnections = require('../Connection/DWH/');

let sqlPurchasingConnection = require('../Connection/Purchasing')
let sqlCoreConnection = require('../Connection/Core/');
let moment = require('moment');


module.exports = async function () {
    return await joinInternNoteCurrency()
        .then((data) => transform(data))
        .then((data) => load(data));
}

const extractInternNote = function () {
    return sqlPurchasingConnection
        .sqlPURCHASING
        .query(`select
        g.id,
        g.lastModifiedUtc,
        g.isDeleted,
        g.inNo,
        g.inDate,
        g.supplierId,
        g.supplierCode,
        g.currencyCode,
        gid.doDate,
        gid.roNo,
        g.supplierName,
        gid.pricePerDealUnit,
        gid.quantity
        from garmentinternnotes g left join GarmentInternNoteItems gi on g.Id = gi.GarmentINId left join GarmentInternNoteDetails gid on gi.Id = gid.GarmentItemINId`, {
            type: sqlPurchasingConnection.sqlPURCHASING.QueryTypes.SELECT
        });

};

const extractGarmentCurrencies = function () {
    return sqlCoreConnection
        .sqlCore
        .query(`select
            id,
            rate,
            date,
            code,
            _IsDeleted
            from garmentcurrencies
            where _IsDeleted = 0 `, {
            type: sqlCoreConnection.sqlCore.QueryTypes.SELECT
        });
}

const joinInternNoteCurrency = function () {
    var interNotes = extractInternNote();
    var currencies = extractGarmentCurrencies();

    return Promise.all([interNotes, currencies])
        .then(async (data) => {
            var interNotes = data[0];
            var currencies = data[1];
            var results = [];
            for (var item of interNotes) {
                var currency = currencies.filter(x => x.code == item.currencyCode &&
                    x.date <= item.doDate)
                    .sort((a, b) => (b.date - a.date))[0];

                var result = {};
                result.internNote = item;

                if (currency) {
                    result.currency = currency;
                } else {
                    result.currency = null;
                }
                var purchaseRequest = await joinPurchaseRequest(item);
                if (purchaseRequest)
                    result.purchaseRequest = purchaseRequest;
                else
                    result.purchaseRequest = null;
                results.push(result);
            }
            return Promise.resolve(results);
        });
}

const joinPurchaseRequest = async function (data) {
    var garmentPRs = await sqlPurchasingConnection
        .sqlPURCHASING
        .query(`select
        g.Id,
        g.IsDeleted,
        g.rono,
        gi.CategoryId,
        gi.CategoryName,
        g.UnitId,
        g.UnitName
        from GarmentPurchaseRequests g left join GarmentPurchaseRequestItems gi on g.Id = gi.GarmentPRId
        where g.RONo = ?`, {
            replacements: [data.roNo],
            type: sqlPurchasingConnection.sqlPURCHASING.QueryTypes.SELECT
        });

    var garmentPR = garmentPRs[0];
    if (garmentPR) {
        var category = await getCategory(garmentPR);
        if (category) {
            garmentPR.categoryCode = category.code;
        }
        var division = await getDivision(garmentPR);
        if (division) {
            garmentPR.divisionId = division.divisionId;
            garmentPR.divisionName = division.divisionName;
        }
    }
    return garmentPR;
}

const getCategory = async function (data) {
    var categories = await sqlCoreConnection
        .sqlCore
        .query(`select 
        id,
        code
        from GarmentCategories
        where id = ?`, {
            replacements: [data.CategoryId],
            type: sqlCoreConnection.sqlCore.QueryTypes.SELECT
        });

    return categories[0];
}

const getDivision = async function (data) {
    var division = await sqlCoreConnection
        .sqlCore
        .query(`select
        id,
        code,
        divisionId,
        divisionName
        from Units
        where id = ?`, {
            replacements: [data.UnitId],
            type: sqlCoreConnection.sqlCore.QueryTypes.SELECT
        });
    return division[0];
}

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

function transform(data) {
    var result = data.map((datum) => {
        var kursCurrency = datum.currency && datum.currency.rate ? datum.currency.rate : 1;
        var purchaseRequest = datum.purchaseRequest;
        var internNote = datum.internNote;

        return {
            deleted: `'${internNote.isDeleted}'`,
            internNoteNo: internNote && internNote.inNo ? `'${internNote.inNo}'` : null,
            date: internNote && internNote.inDate ? `'${moment(internNote.inDate).add(7, "h").format("YYYY-MM-DD")}'` : null,
            suppllierName: internNote && internNote.supplierName ? `'${internNote.supplierName.replace(/'/g, '"')}'` : null,
            categoryType: purchaseRequest && purchaseRequest.CategoryName ? `'${getCategoryType(purchaseRequest.categoryCode)}'` : null,
            invoicePrice: internNote && internNote.pricePerDealUnit ? `${internNote.pricePerDealUnit}` : null,
            deliveredQuantity: internNote && internNote.quantity ? `${internNote.quantity}` : null,
            dealRate: kursCurrency ? `${kursCurrency}` : null,
            totalPrice: kursCurrency && internNote && internNote.quantity && internNote.pricePerDealUnit ? `${kursCurrency * internNote.quantity * internNote.pricePerDealUnit}` : null,
            totalPayment: null,
            categoryName: purchaseRequest && purchaseRequest.CategoryName ? `'${purchaseRequest.CategoryName}'` : null,
            divisionName: purchaseRequest && purchaseRequest.divisionName ? `'${purchaseRequest.divisionName}'` : null,
            unitName: purchaseRequest && purchaseRequest.UnitName ? `'${purchaseRequest.UnitName}'` : null,
        }
    });
    return Promise.resolve(result);
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
                var sqlQuery = 'INSERT INTO [DL_Fact_Total_Hutang_Garment_Temp] ';

                var count = 1;
                for (var item of data) {
                    if (item) {
                        var queryString = `\nSELECT ${item.deleted}, ${item.internNoteNo}, ${item.suppllierName}, ${item.categoryType}, ${item.invoicePrice}, ${item.deliveredQuantity}, ${item.dealRate}, ${item.totalPrice}, ${item.totalPayment}, ${item.categoryName}, ${item.divisionName}, ${item.unitName}, ${item.date} UNION ALL `;
                        sqlQuery = sqlQuery.concat(queryString);
                        if (count % 1000 == 0) {
                            sqlQuery = sqlQuery.substring(0, sqlQuery.length - 10);
                            command.push(insertQuery(sqlDWHConnections.sqlDWH, sqlQuery, t));
                            sqlQuery = 'INSERT INTO [DL_Fact_Total_Hutang_Garment_Temp] ';
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
                        sqlDWHConnections.sqlDWH.query("exec [DL_UPSERT_FACT_GARMENT_TOTAL_HUTANG]", {
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
