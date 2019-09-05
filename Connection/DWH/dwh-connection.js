let Sequelize = require('sequelize');

const sequelize = new Sequelize(process.env.SQLDWH_DATABASE, process.env.SQLDWH_USERNAME, process.env.SQLDWH_PASSWORD, {
    host: process.env.SQLDWH_HOST,
    dialect: 'mssql',
    dialectOptions: {
        options: {
            encrypt: true
        }
    }
});

module.exports = sequelize;