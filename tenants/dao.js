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

const copyTenant = async function(source, destination) {
    const query = `
      SELECT *
      FROM "Tenant"
      WHERE "alias" = ?
      LIMIT ${clientOptions.fetchSize}`;
    const insertQuery = `INSERT into "Tenant" (
      "alias",
      "active",
      "countryCode",
      "displayName",
      "emailDomains",
      "host")
      VALUES (?, ?, ?, ?, ?, ?)`;

    let fetchedRows = await fetchTenants(source, query);
    await insertTenants(destination, fetchedRows, insertQuery);
    let insertedRows = await fetchTenants(destination, query);
    util.compareResults(fetchedRows.rows.length, insertedRows.rows.length);
};

const insertTenants = async function(target, data, insertQuery) {
    if (_.isEmpty(data.rows)) {
        return;
    }
    await (async (targetClient, rows) => {
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
    })(target.client, data.rows);
};

const fetchTenants = async function(target, query) {
    let result = await target.client.execute(query, [
        target.database.tenantAlias
    ]);
    logger.info(
        `${chalk.green(`✓`)}  Fetched ${result.rows.length} Tenant rows...`
    );
    return result;
};

const fetchConfig = async function(target, query) {
    let result = await target.client.execute(query, [
        target.database.tenantAlias
    ]);
    logger.info(
        `${chalk.green(`✓`)}  Fetched ${result.rows.length} Config rows...`
    );
    return result;
};

const insertConfig = async function(target, result, insertQuery) {
    if (_.isEmpty(result.rows)) {
        return;
    }
    await (async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];

            await targetClient.execute(insertQuery, [
                row.tenantAlias,
                row.configKey,
                row.value
            ]);
        }
    })(target.client, result.rows);
};

const copyTenantConfig = async function(source, destination) {
    const query = `
      SELECT *
      FROM "Config"
      WHERE "tenantAlias" = ?
      LIMIT ${clientOptions.fetchSize}`;
    const insertQuery = `
      INSERT INTO "Config" (
      "tenantAlias",
      "configKey",
      value)
      VALUES (?, ?, ?)`;

    let fetchedRows = await fetchConfig(source, query);
    await insertConfig(destination, fetchedRows, insertQuery);
    let insertedRows = await fetchConfig(destination, query);
    util.compareResults(fetchedRows.rows.length, insertedRows.rows.length);
};

module.exports = {
    copyTenant,
    copyTenantConfig
};
