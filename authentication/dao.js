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
const util = require("../util");

const clientOptions = {
    fetchSize: 999999,
    prepare: true
};

const copyAuthenticationUserLoginId = async function(
    sourceClient,
    targetClient
) {
    let query = `SELECT * FROM "AuthenticationUserLoginId" WHERE "userId" IN ?`;
    let insertQuery = `INSERT INTO "AuthenticationUserLoginId" ("userId", "loginId", "value") VALUES (?, ?, ?)`;
    let counter = 0;

    let result = await sourceClient.execute(
        query,
        [store.tenantPrincipals],
        clientOptions
    );
    store.allLoginIds = _.pluck(result.rows, "loginId");

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            let row = rows[i];
            counter++;

            await targetClient.execute(
                insertQuery,
                [row.userId, row.loginId, row.value],
                clientOptions
            );
        }
    }

    logger.info(
        `${chalk.green(`✓`)}  Fetched ${
            result.rows.length
        } AuthenticationUserLoginId rows...`
    );
    if (_.isEmpty(result.rows)) {
        return;
    }
    await insertAll(targetClient, result.rows);
    logger.info(
        `${chalk.green(
            `✓`
        )}  Inserted ${counter} AuthenticationUserLoginId rows...`
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

const copyAuthenticationLoginId = async function(sourceClient, targetClient) {
    if (_.isEmpty(store.allLoginIds)) {
        logger.info(
            chalk.cyan(`✗  Skipped fetching AuthentiationLoginId rows...\n`)
        );
        return [];
    }
    let query = `SELECT * FROM "AuthenticationLoginId" WHERE "loginId" IN ?`;
    let insertQuery = `INSERT INTO "AuthenticationLoginId" ("loginId", password, secret, "userId") VALUES (?, ?, ?, ?)`;
    let counter = 0;

    let result = await sourceClient.execute(
        query,
        [store.allLoginIds],
        clientOptions
    );

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            let row = rows[i];
            counter++;

            await targetClient.execute(
                insertQuery,
                [row.loginId, row.password, row.secret, row.userId],
                clientOptions
            );
        }
    }
    logger.info(
        `${chalk.green(`✓`)}  Fetched ${
            result.rows.length
        } AuthenticationLoginId rows...`
    );
    if (_.isEmpty(result.rows)) {
        return;
    }
    await insertAll(targetClient, result.rows);
    logger.info(
        `${chalk.green(
            `✓`
        )}  Inserted ${counter} AuthenticationUserLoginId rows...`
    );

    let queryResultOnSource = result;
    result = await targetClient.execute(
        query,
        [store.allLoginIds],
        clientOptions
    );
    util.compareBothTenants(
        queryResultOnSource.rows.length,
        result.rows.length
    );
};

const copyOAuthClients = async function(sourceClient, targetClient) {
    if (_.isEmpty(store.allOauthClientsIds)) {
        logger.info(chalk.cyan(`✗  Skipped fetching OAuthClient rows...\n`));
        return [];
    }

    let query = `SELECT * FROM "OAuthClient" WHERE id IN ?`;
    let insertQuery = `INSERT INTO "OAuthClient" (id, "displayName", secret, "userId") VALUES (?, ?, ?, ?)`;
    let counter = 0;

    let result = await sourceClient.execute(
        query,
        [store.allOauthClientsIds],
        clientOptions
    );

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            let row = rows[i];
            counter++;

            await targetClient.execute(
                insertQuery,
                [row.id, row.displayName, row.secret, row.userId],
                clientOptions
            );
        }
    }

    logger.info(
        `${chalk.green(`✓`)}  Fetched ${result.rows.length} OAuthClient rows...`
    );
    if (_.isEmpty(result.rows)) {
        return;
    }
    await insertAll(targetClient, result.rows);
    logger.info(`${chalk.green(`✓`)}  Inserted ${counter} OAuthClient rows...`);

    let queryResultOnSource = result;
    result = await targetClient.execute(
        query,
        [store.allOauthClientsIds],
        clientOptions
    );
    util.compareBothTenants(
        queryResultOnSource.rows.length,
        result.rows.length
    );
};

const copyOAuthClientsByUser = async function(sourceClient, targetClient) {
    let query = `SELECT * FROM "OAuthClientsByUser" WHERE "userId" IN ?`;
    let insertQuery = `INSERT INTO "OAuthClientsByUser" ("userId", "clientId", value) VALUES (?, ?, ?)`;
    let counter = 0;

    let result = await sourceClient.execute(
        query,
        [store.tenantUsers],
        clientOptions
    );
    store.allOauthClientsIds = _.pluck(result.rows, "clientId");

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            let row = rows[i];
            counter++;

            await targetClient.execute(
                insertQuery,
                [row.userId, row.clientId, row.value],
                clientOptions
            );
        }
    }

    logger.info(
        `${chalk.green(`✓`)}  Fetched ${
            result.rows.length
        } OAuthClientsByUser rows...`
    );
    if (_.isEmpty(result.rows)) {
        return;
    }
    await insertAll(targetClient, result.rows);
    logger.info(
        `${chalk.green(`✓`)}  Inserted ${counter} OAuthClientsByUser rows...`
    );

    let queryResultOnSource = result;
    result = await targetClient.execute(
        query,
        [store.tenantUsers],
        clientOptions
    );
    util.compareBothTenants(
        queryResultOnSource.rows.length,
        result.rows.length
    );
};

module.exports = {
    copyAuthenticationLoginId,
    copyAuthenticationUserLoginId,
    copyOAuthClients,
    copyOAuthClientsByUser
};
