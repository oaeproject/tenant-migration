// @format
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
const { Store } = require("../store");

const clientOptions = {
    fetchSize: 999999,
    prepare: true
};

const insertAuthzRoles = async function(target, data, insertQuery) {
    if (_.isEmpty(data.rows)) {
        return;
    }
    await (async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];

            await targetClient.execute(
                insertQuery,
                [row.principalId, row.resourceId, row.role],
                clientOptions
            );
        }
    })(target.client, data.rows);
};

const fetchAuthzRoles = async function(target, query) {
    const _isManager = role => {
        return role === "manager";
    };

    const _belongsToOtherTenant = resourceId => {
        return resourceId.split(":")[1] !== target.database.tenantAlias;
    };

    let result = await target.client.execute(
        query,
        [Store.getAttribute("tenantPrincipals")],
        clientOptions
    );
    // experimental: lets filter resources so that only the tenant's own resources are stored
    let movedResources = [];
    _.each(result.rows, eachRow => {
        if (
            _belongsToOtherTenant(eachRow.resourceId) &&
            _isManager(eachRow.role)
        ) {
            let oldResourceId = eachRow.resourceId;
            eachRow.resourceId = [
                eachRow.resourceId.split(":")[0],
                target.database.tenantAlias,
                eachRow.resourceId.split(":")[2]
            ].join(":");

            // I need to store these so that I can later copy the correspondent files
            movedResources.push(oldResourceId);
        }
    });
    Store.setAttribute("movedResources", movedResources);

    // lets move the tenancy of this resource in case it's a manager
    let allResources = _.filter(result.rows, eachResource => {
        return (
            eachResource.resourceId.split(":")[1] ===
            target.database.tenantAlias
        );
    });
    allResourceIds = _.pluck(allResources, "resourceId");
    Store.setAttribute("allResourceIds", _.uniq(allResourceIds));
    logger.info(
        `${chalk.green(`✓`)}  Fetched ${result.rows.length} AuthzRoles rows...`
    );
    return result;
};

const copyAuthzRoles = async function(source, destination) {
    const query = `
      SELECT *
      FROM "AuthzRoles"
      WHERE "principalId"
      IN ?
      LIMIT ${clientOptions.fetchSize}`;
    const insertQuery = `
      INSERT INTO "AuthzRoles" (
          "principalId",
          "resourceId",
          role)
          VALUES (?, ?, ?)`;

    let fetchedRows = await fetchAuthzRoles(source, query);
    await insertAuthzRoles(destination, fetchedRows, insertQuery);
    let insertedRows = await fetchAuthzRoles(destination, query);
    util.compareResults(fetchedRows.rows.length, insertedRows.rows.length);
};

const insertAuthzMembers = async function(target, data, insertQuery) {
    if (_.isEmpty(data.rows)) {
        return;
    }
    await (async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];

            await targetClient.execute(
                insertQuery,
                [row.resourceId, row.memberId, row.role],
                clientOptions
            );
        }
    })(target.client, data.rows);
};

const fetchAuthzMembers = async function(target, query) {
    let result = await target.client.execute(
        query,
        [Store.getAttribute("tenantPrincipals")],
        clientOptions
    );
    logger.info(
        `${chalk.green(`✓`)}  Fetched ${
            result.rows.length
        } AuthzMembers rows...`
    );

    return result;
};

const copyAuthzMembers = async function(source, destination) {
    const query = `
      SELECT *
      FROM "AuthzMembers"
      WHERE "memberId"
      IN ?
      LIMIT ${clientOptions.fetchSize} ALLOW FILTERING`;
    const insertQuery = `
      INSERT INTO "AuthzMembers" (
          "resourceId",
          "memberId",
          role)
          VALUES (?, ?, ?)`;

    let fetchedRows = await fetchAuthzMembers(source, query);
    await insertAuthzMembers(destination, fetchedRows, insertQuery);
    let insertedRows = await fetchAuthzMembers(destination, query);
    util.compareResults(fetchedRows.rows.length, insertedRows.rows.length);
};

module.exports = {
    copyAuthzMembers,
    copyAuthzRoles
};
