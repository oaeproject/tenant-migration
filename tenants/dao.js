/*!
 * Copyright 2018 Apereo Foundation (AF) Licensed under the
 * Educational Community License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 *     http://opensource.org/licenses/ECL-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

const chalk = require("chalk");
const _ = require("underscore");
const logger = require("../logger");
let store = require("../store");
let sourceDatabase = store.sourceDatabase;

const selectAllTenants = function(sourceClient) {
    let query = `select * from "Tenant" where "alias" = ?`;
    return sourceClient.execute(query, [sourceDatabase.tenantAlias]);
};

const insertAllTenants = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(`${chalk.green(`✓`)}  No Tenant rows found...`);
        return;
    }

    let row = result.first();
    let insertQuery = `INSERT into "Tenant" ("alias", "active", "countryCode", "displayName", "emailDomains", "host") VALUES (?, ?, ?, ?, ?, ?)`;

    logger.info(`${chalk.green(`✓`)}  Inserting tenant...`);
    await targetClient.execute(insertQuery, [
        row.alias,
        row.active,
        row.countryCode,
        row.displayName,
        row.emailDomains,
        row.host
    ]);
};

const selectTenantConfig = function(sourceClient) {
    let query = `SELECT * FROM "Config" WHERE "tenantAlias" = '${
        sourceDatabase.tenantAlias
    }'`;
    return sourceClient.execute(query);
};

const insertTenantConfig = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(`${chalk.green(`✓`)}  No Config rows found...`);
        return;
    }

    let row = result.first();
    logger.info(`${chalk.green(`✓`)}  Inserting tenant config...`);
    let insertQuery = `INSERT INTO "Config" ("tenantAlias", "configKey", value) VALUES (?, ?, ?)`;
    return targetClient.execute(insertQuery, [
        row.tenantAlias,
        row.configKey,
        row.configKey
    ]);
};

module.exports = {
    selectAllTenants,
    insertAllTenants,
    selectTenantConfig,
    insertTenantConfig
};
