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
const { Store } = require("../store");
const util = require("../util");

const clientOptions = {
    fetchSize: 999999,
    prepare: true
};

const copyAuthenticationUserLoginId = async function(source, target) {
    const query = `SELECT * FROM "AuthenticationUserLoginId" WHERE "userId" IN ? limit ${
        clientOptions.fetchSize
    }`;
    const insertQuery = `INSERT INTO "AuthenticationUserLoginId" ("userId", "loginId", "value") VALUES (?, ?, ?)`;

    let result = await source.client.execute(
        query,
        [Store.getAttribute("tenantPrincipals")],
        clientOptions
    );
    let allLoginIds = _.pluck(result.rows, "loginId");
    Store.setAttribute("allLoginIds", _.uniq(allLoginIds));

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];

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
    await insertAll(target.client, result.rows);

    const queryResultOnSource = result;
    result = await target.client.execute(
        query,
        [Store.getAttribute("tenantPrincipals")],
        clientOptions
    );
    util.compareResults(queryResultOnSource.rows.length, result.rows.length);
};

const copyAuthenticationLoginId = async function(source, target) {
    if (_.isEmpty(Store.getAttribute("allLoginIds"))) {
        logger.info(
            chalk.cyan(`✗  Skipped fetching AuthentiationLoginId rows...\n`)
        );
        return [];
    }
    const query = `SELECT * FROM "AuthenticationLoginId" WHERE "loginId" IN ? LIMIT ${
        clientOptions.fetchSize
    }`;
    const insertQuery = `INSERT INTO "AuthenticationLoginId" ("loginId", password, secret, "userId") VALUES (?, ?, ?, ?)`;

    let result = await source.client.execute(
        query,
        [Store.getAttribute("allLoginIds")],
        clientOptions
    );

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];

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
    await insertAll(target.client, result.rows);

    const queryResultOnSource = result;
    result = await target.client.execute(
        query,
        [Store.getAttribute("allLoginIds")],
        clientOptions
    );
    util.compareResults(queryResultOnSource.rows.length, result.rows.length);
};

const copyOAuthClients = async function(source, target) {
    if (_.isEmpty(Store.getAttribute("allOauthClientsIds"))) {
        logger.info(chalk.cyan(`✗  Skipped fetching OAuthClient rows...\n`));
        return [];
    }

    const query = `SELECT * FROM "OAuthClient" WHERE id IN ? LIMIT ${
        clientOptions.fetchSize
    }`;
    const insertQuery = `INSERT INTO "OAuthClient" (id, "displayName", secret, "userId") VALUES (?, ?, ?, ?)`;

    let result = await source.client.execute(
        query,
        [Store.getAttribute("allOauthClientsIds")],
        clientOptions
    );

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];

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
    await insertAll(target.client, result.rows);

    const queryResultOnSource = result;
    result = await target.client.execute(
        query,
        [Store.getAttribute("allOauthClientsIds")],
        clientOptions
    );
    util.compareResults(queryResultOnSource.rows.length, result.rows.length);
};

const copyOAuthClientsByUser = async function(source, target) {
    const query = `SELECT * FROM "OAuthClientsByUser" WHERE "userId" IN ? LIMIT ${
        clientOptions.fetchSize
    }`;
    const insertQuery = `INSERT INTO "OAuthClientsByUser" ("userId", "clientId", value) VALUES (?, ?, ?)`;

    let result = await source.client.execute(
        query,
        [Store.getAttribute("tenantUsers")],
        clientOptions
    );
    let allOauthClientsIds = _.pluck(result.rows, "clientId");
    Store.setAttribute("allOauthClientsIds", _.uniq(allOauthClientsIds));

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];

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
    await insertAll(target.client, result.rows);

    const queryResultOnSource = result;
    result = await target.client.execute(
        query,
        [Store.getAttribute("tenantUsers")],
        clientOptions
    );
    util.compareResults(queryResultOnSource.rows.length, result.rows.length);
};

module.exports = {
    copyAuthenticationLoginId,
    copyAuthenticationUserLoginId,
    copyOAuthClients,
    copyOAuthClientsByUser
};
