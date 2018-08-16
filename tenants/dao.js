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

const util = require("../util");

const clientOptions = {
    fetchSize: 999999,
    prepare: true
};

const copyAllTenants = async function(source, target) {
    const query = `select * from "Tenant" where "alias" = ? LIMIT ${
        clientOptions.fetchSize
    }`;
    let result = await source.client.execute(query, [
        source.database.tenantAlias
    ]);

    const insertQuery = `INSERT into "Tenant" ("alias", "active", "countryCode", "displayName", "emailDomains", "host") VALUES (?, ?, ?, ?, ?, ?)`;

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];

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

    await insertAll(target.client, result.rows);

    const queryResultOnSource = result;
    result = await target.client.execute(query, [source.database.tenantAlias]);
    util.compareBothTenants(
        queryResultOnSource.rows.length,
        result.rows.length
    );
};

const copyTenantConfig = async function(source, target) {
    const query = `SELECT * FROM "Config" WHERE "tenantAlias" = '${
        source.database.tenantAlias
    }' LIMIT ${clientOptions.fetchSize}`;
    const insertQuery = `INSERT INTO "Config" ("tenantAlias", "configKey", value) VALUES (?, ?, ?)`;

    let result = await source.client.execute(query);

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];

            await targetClient.execute(insertQuery, [
                row.tenantAlias,
                row.configKey,
                row.value
            ]);
        }
    }

    logger.info(
        `${chalk.green(`✓`)}  Fetched ${result.rows.length} Config rows...`
    );
    if (_.isEmpty(result.rows)) {
        return;
    }

    await insertAll(target.client, result.rows);

    const queryResultOnSource = result;
    result = await target.client.execute(query);
    util.compareBothTenants(
        queryResultOnSource.rows.length,
        result.rows.length
    );
};

module.exports = {
    copyAllTenants,
    copyTenantConfig
};
