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
const util = require("../util");

const clientOptions = {
    fetchSize: 999999,
    prepare: true
};

const copyAllTenants = async function(sourceClient, targetClient) {
    let query = `select * from "Tenant" where "alias" = ? LIMIT ${
        clientOptions.fetchSize
    }`;
    let result = await sourceClient.execute(query, [
        sourceDatabase.tenantAlias
    ]);

    let counter = 0;
    let insertQuery = `INSERT into "Tenant" ("alias", "active", "countryCode", "displayName", "emailDomains", "host") VALUES (?, ?, ?, ?, ?, ?)`;

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            let row = rows[i];
            counter++;

            await targetClient.execute(insertQuery, [
                row.alias,
                row.active,
                row.countryCode,
                row.displayName,
                row.emailDomains,
                row.host
            ]);
        }
    }

    logger.info(
        `${chalk.green(`✓`)}  Fetched ${result.rows.length} Tenant rows...`
    );
    if (_.isEmpty(result.rows)) {
        return;
    }

    await insertAll(targetClient, result.rows);
    logger.info(`${chalk.green(`✓`)}  Inserted ${counter} Tenant rows...`);

    let queryResultOnSource = result;
    result = await targetClient.execute(query, [sourceDatabase.tenantAlias]);
    util.compareBothTenants(
        queryResultOnSource.rows.length,
        result.rows.length
    );
};

const copyTenantConfig = async function(sourceClient, targetClient) {
    let query = `SELECT * FROM "Config" WHERE "tenantAlias" = '${
        sourceDatabase.tenantAlias
    }' LIMIT ${clientOptions.fetchSize}`;
    let insertQuery = `INSERT INTO "Config" ("tenantAlias", "configKey", value) VALUES (?, ?, ?)`;
    let counter = 0;

    let result = await sourceClient.execute(query);

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            let row = rows[i];
            counter++;

            await targetClient.execute(insertQuery, [
                row.tenantAlias,
                row.configKey,
                row.configKey
            ]);
        }
    }

    logger.info(
        `${chalk.green(`✓`)}  Fetched ${result.rows.length} Config rows...`
    );
    if (_.isEmpty(result.rows)) {
        return;
    }

    await insertAll(targetClient, result.rows);
    logger.info(`${chalk.green(`✓`)}  Inserted ${counter} Config rows...`);

    let queryResultOnSource = result;
    result = await targetClient.execute(query);
    util.compareBothTenants(
        queryResultOnSource.rows.length,
        result.rows.length
    );
};

module.exports = {
    copyAllTenants,
    copyTenantConfig
};
