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

const selectAuthenticationUserLoginId = function(sourceClient) {
    let query = `SELECT * FROM "AuthenticationUserLoginId" WHERE "userId" IN ?`;
    return sourceClient.execute(query, [store.tenantPrincipals]);
};

const insertAuthenticationUserLoginId = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(
            `${chalk.green(`✓`)}  No AuthenticationUserLoginId rows found...`
        );
        return;
    }

    store.allLoginIds = _.pluck(result.rows, "loginId");
    let allInserts = [];
    result.rows.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "AuthenticationUserLoginId" ("userId", "loginId", "value") VALUES (?, ?, ?)`,
            params: [row.userId, row.loginId, row.value]
        });
    });
    logger.info(`${chalk.green(`✓`)}  Inserting AuthenticationUserLoginId...`);
    await targetClient.batch(allInserts, { prepare: true });
};

const selectAuthenticationLoginId = function(sourceClient) {
    if (_.isEmpty(store.allLoginIds)) {
        return [];
    }
    let query = `SELECT * FROM "AuthenticationLoginId" WHERE "loginId" IN ?`;
    return sourceClient.execute(query, [store.allLoginIds]);
};

const insertAuthenticationUserId = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(
            `${chalk.green(`✓`)}  No AuthenticationLoginId rows found...`
        );
        return;
    }

    let allInserts = [];
    result.rows.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "AuthenticationLoginId" ("loginId", password, secret, "userId") VALUES (?, ?, ?, ?)`,
            params: [row.loginId, row.password, row.secret, row.userId]
        });
    });
    logger.info(`${chalk.green(`✓`)}  Inserting AuthenticationUserLoginId...`);
    await targetClient.batch(allInserts, { prepare: true });
};

const selectOAuthClients = function(sourceClient) {
    let query = `SELECT * FROM "OAuthClient" WHERE id IN ?`;
    if (_.isEmpty(store.allOauthClientsIds)) {
        return [];
    }

    return sourceClient.execute(query, [store.allOauthClientsIds]);
};

const insertOAuthClients = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(`${chalk.green(`✓`)}  No OAuthClient rows found...`);
        return;
    }

    let allInserts = [];
    result.rows.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "OAuthClient" (id, "displayName", secret, "userId") VALUES (?, ?, ?, ?)`,
            params: [row.id, row.displayName, row.secret, row.userId]
        });
    });
    logger.info(`${chalk.green(`✓`)}  Inserting OAuthClient...`);
    await targetClient.batch(allInserts, { prepare: true });
};

const selectOAuthClientsByUser = function(sourceClient) {
    let query = `SELECT * FROM "OAuthClientsByUser" WHERE "userId" IN ?`;
    return sourceClient.execute(query, [store.tenantUsers]);
};

const insertOAuthClientsByUser = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(`${chalk.green(`✓`)}  No OAuthClientsByUser rows found...`);
        return;
    }

    store.allOauthClientsIds = _.pluck(result.rows, "clientId");
    let allInserts = [];
    result.rows.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "OAuthClientsByUser" ("userId", "clientId", value) VALUES (?, ?, ?)`,
            params: [row.userId, row.clientId, row.value]
        });
    });
    logger.info(`${chalk.green(`✓`)}  Inserting OAuthClientsByUser...`);
    await targetClient.batch(allInserts, { prepare: true });
};

module.exports = {
    selectAuthenticationLoginId,
    selectAuthenticationUserLoginId,
    selectOAuthClients,
    insertAuthenticationUserId,
    insertAuthenticationUserLoginId,
    insertOAuthClients,
    selectOAuthClientsByUser,
    insertOAuthClientsByUser
};
