let Sequelize = require('sequelize');

const sequelize = new Sequelize(process.env.SQLSales_DATABASE, process.env.SQLAZURE_USERNAME, process.env.SQLAZURE_PASSWORD, {
    host: process.env.SQLAZURE_HOST,
    dialect: 'mssql',
    dialectOptions: {
        options: {
            encrypt: true
        }
    }
});

module.exports = sequelize;