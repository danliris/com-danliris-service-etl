var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://danliris-prd:Standar123.@13.76.130.98:27017/danliris-prd?authSource=danliris-prd";
let sqlDWHConnections = require('../Connection/DWH/');
let sqlFPConnection = require('../Connection/FinishingPrinting/')
const MIGRATION_LOG_DESCRIPTION = "Fact Packing from MongoDB to Azure DWH";
let moment = require('moment');

module.exports = async function (context, req) {
    context.log('JavaScript HTTP trigger function processed a request.');


    await MongoClient.connect(url, async function (err, db) {
        if (err) throw err;
        var dbo = db.db("danliris-prd");
        /*Return only the documents with the address "Park Lane 38":*/
        await dbo.collection("migration-log")
            .aggregate([
                {
                    $match: {
                        "status": { '$in': ['Successful', 'success'] }
                    }
                },
                {
                    $project: {
                        "finish": 1,
                        "description": 1,
                        "start": 1,
                        "status": 1,
                        "executionTime": 1
                    }
                },
                {
                    $sort: { "finish": -1 }
                },
                {
                    $group: {
                        "_id": {
                            "description": "$description"
                        },
                        "record": { $first: "$$ROOT" }
                    }
                }])
            .toArray(function (err, result) {
                if (err) throw err;
                load(result);
                db.close();
            });
    });
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
                var sqlQuery = 'INSERT INTO [migration-log] ';

                var count = 1;
                for (var item of data) {
                    if (item.record) {
                        var datum = item.record;
                        var dateStart = moment(datum.start).format('YYYY-MM-DD');
                        var dateFinish = moment(datum.finish).format('YYYY-MM-DD');
                        var queryString = `\nSELECT '${datum.description}', '${dateStart}', '${dateFinish}', '${datum.executionTime}', '${datum.status}' UNION ALL `;
                        sqlQuery = sqlQuery.concat(queryString);
                        if (count % 1000 == 0) {
                            sqlQuery = sqlQuery.substring(0, sqlQuery.length - 10);
                            command.push(insertQuery(sqlDWHConnections.sqlDWH, sqlQuery, t));
                            sqlQuery = "INSERT INTO [migration-log] ";
                        }
                        console.log(`add data to query  : ${count}`);
                        count++;
                    }
                }


                if (sqlQuery != "" && sqlQuery !== "INSERT INTO [migration-log] ") {
                    sqlQuery = sqlQuery.substring(0, sqlQuery.length - 10);
                    command.push(insertQuery(sqlDWHConnections.sqlDWH, `${sqlQuery}`, t));
                }


                return Promise.all(command)
                    .then((results) => {
                        sqlDWHConnections.sqlDWH.query("exec [DL_UPSERT_FACT_PACKING]", {
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