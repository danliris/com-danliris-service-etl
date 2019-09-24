let sqlDWHConnections = require('../Connection/DWH/');
let sqlSalesConnections = require('../Connection/Sales/')
const MIGRATION_LOG_DESCRIPTION = "Fact Spinning Sales Contract from MongoDB to Azure DWH";
let moment = require('moment');

module.exports = async function () {
    var startedDate = new Date();
    return await timestamp()
        .then((times) => extract(times))
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

const extract = async function (times) {
    var time = times.length > 0 ? moment(times[0].start).format("YYYY-MM-DD") : "1970-01-01";
    var timestamp = new Date(time);
    return await sqlSalesConnections
        .sqlSales
        .query(`SELECT  
        salesContractNo, 
        CreatedUtc _createdDate,
        buyerName,
        buyerType, 
        orderQuantity, 
        uomUnit,
        buyerCode,
        IsDeleted _deleted
        FROM spinningsalescontract
        where lastmodifiedutc > ?
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
    var result = data.map((item) => {
        var orderUom = item.uomUnit ? item.uomUnit : null;
        var orderQuantity = item.orderQuantity ? item.orderQuantity : null;
        var material = item.materialName ? item.materialName.replace(/'/g, '"') : null;
        var materialConstruction = item.materialConstructionName ? item.materialConstructionName.replace(/'/g, '"') : null;
        var yarnMaterialNo = item.yarnMaterialName ? item.yarnMaterialName.replace(/'/g, '"') : null;
        var materialWidth = item.materialWidth ? item.materialWidth : null;

        return {
            salesContractNo: item.salesContractNo ? `'${item.salesContractNo}'` : null,
            salesContractDate: item._createdDate ? `'${moment(item._createdDate).add(7, "hours").format("YYYY-MM-DD")}'` : null,
            deliverySchedule: item.deliverySchedule ? `'${moment(item.deliverySchedule).add(7, "hours").format("YYYY-MM-DD")}'` : null,
            buyer: item.buyerName ? `'${item.buyerName.replace(/'/g, '"')}'` : null,
            buyerType: item.buyerType ? `'${item.buyerType.replace(/'/g, '"')}'` : null,
            orderType: item.orderTypeName ? `'${item.orderTypeName}'` : null,
            orderQuantity: item.orderQuantity ? `${item.orderQuantity}` : null,
            orderUom: item.uomUnit ? `'${item.uomUnit.replace(/'/g, '"')}'` : null,
            totalOrderConvertion: item.orderQuantity ? `${orderQuantityConvertion(orderUom, orderQuantity)}` : null,
            buyerCode: item.buyerCode ? `'${item.buyerCode}'` : null,
            productionType: `'${"Spinning"}'`,
            construction: joinConstructionString(material, materialConstruction, yarnMaterialNo, materialWidth),
            materialConstruction: item.materialConstructionName ? `'${item.materialConstructionName.replace(/'/g, '"')}'` : null,
            materialWidth: item.materialWidth ? `'${item.materialWidth.replace(/'/g, '"')}'` : null,
            material: item.materialName ? `'${item.materialName.replace(/'/g, '"')}'` : null,
            deleted: `'${item._deleted}'`
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
                var sqlQuery = 'INSERT INTO [DL_Fact_Sales_Contract_Temp]([Nomor Sales Contract], [Tanggal Sales Contract], [Buyer], [Jenis Buyer], [Jenis Order], [Jumlah Order], [Satuan], [Jumlah Order Konversi], [Kode Buyer], [Jenis Produksi], [Konstruksi], [Konstruksi Material], [Lebar Material], [Material], [_deleted], [deliverySchedule])';

                var count = 1;
                for (var item of data) {
                    if (item) {
                        var values = `${item.salesContractNo}, ${item.salesContractDate}, ${item.buyer}, ${item.buyerType}, ${item.orderType}, ${item.orderQuantity}, ${item.orderUom}, ${item.totalOrderConvertion}, ${item.buyerCode}, ${item.productionType}, ${item.construction}, ${item.materialConstruction}, ${item.materialWidth}, ${item.material}, ${item.deleted}, ${item.deliverySchedule}`;
                        var queryString = `\nSELECT ${values} UNION ALL `;

                        sqlQuery = sqlQuery.concat(queryString);

                        if (count % 1000 == 0) {
                            sqlQuery = sqlQuery.substring(0, sqlQuery.length - 10);
                            command.push(insertQuery(sqlDWHConnections.sqlDWH, sqlQuery, t));
                            sqlQuery = 'INSERT INTO [DL_Fact_Sales_Contract_Temp]([Nomor Sales Contract], [Tanggal Sales Contract], [Buyer], [Jenis Buyer], [Jenis Order], [Jumlah Order], [Satuan], [Jumlah Order Konversi], [Kode Buyer], [Jenis Produksi], [Konstruksi], [Konstruksi Material], [Lebar Material], [Material], [_deleted], [deliverySchedule])';
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
                        sqlDWHConnections.sqlDWH.query("exec [DL_UPSERT_FACT_SALES_CONTRACT]", {
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