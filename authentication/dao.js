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

/* eslint-disable no-await-in-loop */
const chalk = require('chalk');
const _ = require('underscore');
const logger = require('../logger');
const { Store } = require('../store');
const util = require('../util');

const clientOptions = {
  fetchSize: 999999,
  prepare: true
};

const insertAllAuthenticationUserLoginId = async function(target, data, insertQuery) {
  if (_.isEmpty(data.rows)) {
    return;
  }
  await (async function(targetClient, rows) {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      await targetClient.execute(insertQuery, [row.userId, row.loginId, row.value], clientOptions);
    }
  })(target.client, data.rows);
};

const fetchAllAuthenticationUserLoginId = async function(target, query) {
  const result = await target.client.execute(
    query,
    [Store.getAttribute('tenantPrincipals')],
    clientOptions
  );

  logger.info(
    `${chalk.green(`✓`)}  Fetched ${
      result.rows.length
    } AuthenticationUserLoginId rows from ${chalk.cyan(target.database.host)}`
  );
  return result;
};

const copyAuthenticationUserLoginId = async function(source, destination) {
  const query = `
      SELECT *
      FROM "AuthenticationUserLoginId"
      WHERE "userId"
      IN ? limit ${clientOptions.fetchSize}`;
  const insertQuery = `
      INSERT INTO "AuthenticationUserLoginId" (
      "userId",
      "loginId",
      "value")
      VALUES (?, ?, ?)`;

  const fetchedRows = await fetchAllAuthenticationUserLoginId(source, query);
  Store.setAttribute('allLoginIds', _.uniq(_.pluck(fetchedRows.rows, 'loginId')));
  await insertAllAuthenticationUserLoginId(destination, fetchedRows, insertQuery);

  const insertedRows = await fetchAllAuthenticationUserLoginId(destination, query);
  util.compareResults(fetchedRows.rows.length, insertedRows.rows.length);
};

const insertAllAuthenticationLoginId = async function(target, data, insertQuery) {
  if (_.isEmpty(data.rows)) {
    return;
  }

  await (async function(targetClient, rows) {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      await targetClient.execute(
        insertQuery,
        [row.loginId, row.password, row.secret, row.userId],
        clientOptions
      );
    }
  })(target.client, data.rows);
};

const fetchAllAuthenticationLoginId = async function(target, query) {
  const result = await target.client.execute(
    query,
    [Store.getAttribute('allLoginIds')],
    clientOptions
  );
  logger.info(
    `${chalk.green(`✓`)}  Fetched ${
      result.rows.length
    } AuthenticationLoginId rows from ${chalk.cyan(target.database.host)}`
  );

  return result;
};

const copyAuthenticationLoginId = async function(source, destination) {
  if (_.isEmpty(Store.getAttribute('allLoginIds'))) {
    logger.info(chalk.cyan(`✗  Skipped fetching AuthentiationLoginId rows...\n`));
    return [];
  }
  const query = `
      SELECT *
      FROM "AuthenticationLoginId"
      WHERE "loginId"
      IN ?
      LIMIT ${clientOptions.fetchSize}`;
  const insertQuery = `
      INSERT INTO "AuthenticationLoginId" (
      "loginId",
      password,
      secret,
      "userId")
      VALUES (?, ?, ?, ?)`;

  const fetchedRows = await fetchAllAuthenticationLoginId(source, query);
  await insertAllAuthenticationLoginId(destination, fetchedRows, insertQuery);

  const insertedRows = await fetchAllAuthenticationLoginId(destination, query);
  util.compareResults(fetchedRows.rows.length, insertedRows.rows.length);
};

const insertAllOAuthClients = async function(target, data, insertQuery) {
  if (_.isEmpty(data.rows)) {
    return;
  }
  await (async function(targetClient, rows) {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      await targetClient.execute(
        insertQuery,
        [row.id, row.displayName, row.secret, row.userId],
        clientOptions
      );
    }
  })(target.client, data.rows);
};

const fetchAllOAuthClients = async function(target, query) {
  const result = await target.client.execute(
    query,
    [Store.getAttribute('allOauthClientsIds')],
    clientOptions
  );
  logger.info(
    `${chalk.green(`✓`)}  Fetched ${result.rows.length} OAuthClient rows from ${chalk.cyan(
      target.database.host
    )}`
  );

  return result;
};

const copyOAuthClients = async function(source, destination) {
  if (_.isEmpty(Store.getAttribute('allOauthClientsIds'))) {
    logger.info(chalk.cyan(`✗  Skipped fetching OAuthClient rows...\n`));
    return [];
  }

  const query = `
      SELECT *
      FROM "OAuthClient"
      WHERE id
      IN ?
      LIMIT ${clientOptions.fetchSize}`;
  const insertQuery = `
      INSERT INTO "OAuthClient" (
      id,
      "displayName",
      secret,
      "userId")
      VALUES (?, ?, ?, ?)`;

  const fetchedRows = await fetchAllOAuthClients(source, query);
  await insertAllOAuthClients(destination, fetchedRows, insertQuery);

  const insertedRows = await fetchAllOAuthClients(destination, query);
  util.compareResults(fetchedRows.rows.length, insertedRows.rows.length);
};

const insertAllOAuthClientsByUser = async function(target, data, insertQuery) {
  if (_.isEmpty(data.rows)) {
    return;
  }
  await (async function(targetClient, rows) {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      await targetClient.execute(insertQuery, [row.userId, row.clientId, row.value], clientOptions);
    }
  })(target.client, data.rows);
};

const fetchAllOAuthClientsByUser = async function(target, query) {
  const result = await target.client.execute(
    query,
    [Store.getAttribute('tenantUsers')],
    clientOptions
  );

  logger.info(
    `${chalk.green(`✓`)}  Fetched ${result.rows.length} OAuthClientsByUser rows from ${chalk.cyan(
      target.database.host
    )}`
  );
  return result;
};

const copyOAuthClientsByUser = async function(source, destination) {
  const query = `
      SELECT *
      FROM "OAuthClientsByUser"
      WHERE "userId"
      IN ?
      LIMIT ${clientOptions.fetchSize}`;
  const insertQuery = `
      INSERT INTO "OAuthClientsByUser" (
      "userId",
      "clientId",
      value)
      VALUES (?, ?, ?)`;

  const fetchedRows = await fetchAllOAuthClientsByUser(source, query);
  Store.setAttribute('allOauthClientsIds', _.uniq(_.pluck(fetchedRows.rows, 'clientId')));
  await insertAllOAuthClientsByUser(destination, fetchedRows, insertQuery);

  const insertedRows = await fetchAllOAuthClientsByUser(destination, query);
  util.compareResults(fetchedRows.rows.length, insertedRows.rows.length);
};

module.exports = {
  copyAuthenticationLoginId,
  copyAuthenticationUserLoginId,
  copyOAuthClients,
  copyOAuthClientsByUser
};
