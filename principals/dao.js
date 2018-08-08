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

const copyAllPrincipals = async function(sourceClient, targetClient) {
    let query = `SELECT * FROM "Principals" WHERE "tenantAlias" = ? LIMIT ${
        clientOptions.fetchSize
    }`;
    let insertQuery = `INSERT INTO "Principals" ("principalId", "acceptedTC", "admin:global", "admin:tenant", created, "createdBy", deleted, description, "displayName", email, "emailPreference", joinable, "largePictureUri", "lastModified", locale, "mediumPictureUri", "notificationsLastRead", "notificationsUnread", "publicAlias", "smallPictureUri", "tenantAlias", visibility) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
    let counter = 0;

    let result = await sourceClient.execute(
        query,
        [sourceDatabase.tenantAlias],
        clientOptions
    );

    // we'll need to know which principals are users or groups
    result.rows.forEach(row => {
        store.tenantPrincipals.push(row.principalId);
        if (row.principalId.startsWith("g")) {
            store.tenantGroups.push(row.principalId);
        } else if (row.principalId.startsWith("u")) {
            store.tenantUsers.push(row.principalId);
        }
    });

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            let row = rows[i];
            counter++;

            await targetClient.execute(
                insertQuery,
                [
                    row.principalId,
                    row.acceptedTC,
                    row.get("admin:global"),
                    row.get("admin:tenant"),
                    row.created,
                    row.createdBy,
                    row.deleted,
                    row.description,
                    row.displayName,
                    row.email,
                    row.emailPreference,
                    row.joinable,
                    row.largePictureUri,
                    row.lastModified,
                    row.locale,
                    row.mediumPictureUri,
                    row.notificationsLastRead,
                    row.notificationsUnread,
                    row.publicAlias,
                    row.smallPictureUri,
                    row.tenantAlias,
                    row.visibility
                ],
                clientOptions
            );
        }
    }

    logger.info(
        `${chalk.green(`✓`)}  Fetched ${result.rows.length} Principals rows...`
    );
    if (_.isEmpty(result.rows)) {
        return;
    }
    await insertAll(targetClient, result.rows);
    logger.info(`${chalk.green(`✓`)}  Inserted ${counter} Principals rows...`);

    let queryResultOnSource = result;
    result = await targetClient.execute(
        query,
        [sourceDatabase.tenantAlias],
        clientOptions
    );
    util.compareBothTenants(
        queryResultOnSource.rows.length,
        result.rows.length
    );
};

const copyPrincipalsByEmail = async function(sourceClient, targetClient) {
    let query = `SELECT * FROM "PrincipalsByEmail" WHERE "principalId" IN ? LIMIT ${
        clientOptions.fetchSize
    } ALLOW FILTERING`;
    let insertQuery = `INSERT INTO "PrincipalsByEmail" (email, "principalId") VALUES (?, ?)`;
    let counter = 0;

    let result = await sourceClient.execute(
        query,
        [store.tenantPrincipals],
        clientOptions
    );

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            let row = rows[i];
            counter++;

            await targetClient.execute(
                insertQuery,
                [row.email, row.principalId],
                clientOptions
            );
        }
    }

    logger.info(
        `${chalk.green(`✓`)}  Fetched ${
            result.rows.length
        } PrincipalsByEmail rows...`
    );
    if (_.isEmpty(result.rows)) {
        return;
    }
    await insertAll(targetClient, result.rows);
    logger.info(
        `${chalk.green(`✓`)}  Inserted ${counter} PrincipalsByEmail rows...`
    );

    let queryResultOnSource = result;
    result = await targetClient.execute(
        query,
        [store.tenantPrincipals],
        clientOptions
    );
    util.compareBothTenants(
        queryResultOnSource.rows.length,
        result.rows.length
    );
};

module.exports = {
    copyAllPrincipals,
    copyPrincipalsByEmail
};
