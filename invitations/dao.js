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

const insertAuthzInvitations = async function(target, data, insertQuery) {
  if (_.isEmpty(data.rows)) {
    return;
  }

  await (async function(targetClient, rows) {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      await targetClient.execute(
        insertQuery,
        [row.resourceId, row.email, row.inviterUserId, row.role],
        clientOptions
      );
    }
  })(target.client, data.rows);
};

const fetchAuthzInvitations = async function(target, query) {
  const result = await target.client.execute(
    query,
    [Store.getAttribute('allResourceIds')],
    clientOptions
  );
  logger.info(
    `${chalk.green(`✓`)}  Fetched ${result.rows.length} AuthzInvitations rows from ${chalk.cyan(
      target.database.host
    )}`
  );

  return result;
};

const copyAuthzInvitations = async function(source, destination) {
  const query = `
      SELECT *
      FROM "AuthzInvitations"
      WHERE "resourceId"
      IN ?
      LIMIT ${clientOptions.fetchSize}`;
  const insertQuery = `
      INSERT INTO "AuthzInvitations" (
      "resourceId",
      email,
      "inviterUserId",
      role)
      VALUES (?, ?, ?, ?)`;

  const fetchedRows = await fetchAuthzInvitations(source, query);
  Store.setAttribute('allInvitationEmails', _.uniq(_.pluck(fetchedRows.rows, 'email')));
  await insertAuthzInvitations(destination, fetchedRows, insertQuery);

  const insertedRows = await fetchAuthzInvitations(destination, query);
  util.compareResults(fetchedRows.rows.length, insertedRows.rows.length);
};

const insertAuthzInvitationsResourceIdByEmail = async function(target, data, insertQuery) {
  if (_.isEmpty(data.rows)) {
    return;
  }
  await (async function(targetClient, rows) {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      await targetClient.execute(insertQuery, [row.email, row.resourceId], clientOptions);
    }
  })(target.client, data.rows);
};

const fetchAuthzInvitationsResourceIdByEmail = async function(target, query) {
  const result = await target.client.execute(
    query,
    [Store.getAttribute('allInvitationEmails')],
    clientOptions
  );

  logger.info(
    `${chalk.green(`✓`)}  Fetched ${
      result.rows.length
    } AuthzInvitationsResourceIdByEmail rows from ${chalk.cyan(target.database.host)}`
  );
  return result;
};

const copyAuthzInvitationsResourceIdByEmail = async function(source, destination) {
  if (_.isEmpty(Store.getAttribute('allInvitationEmails'))) {
    logger.info(chalk.cyan(`✗  Skipped fetching AuthzInvitationsResourceIdByEmail rows...\n`));
    return [];
  }

  const query = `
      SELECT *
      FROM "AuthzInvitationsResourceIdByEmail"
      WHERE email
      IN ?
      LIMIT ${clientOptions.fetchSize}`;
  const insertQuery = `
      INSERT INTO "AuthzInvitationsResourceIdByEmail" (
      email,
      "resourceId")
      VALUES (?, ?)`;
  const fetchedRows = await fetchAuthzInvitationsResourceIdByEmail(source, query);
  await insertAuthzInvitationsResourceIdByEmail(destination, fetchedRows, insertQuery);
};

const insertAuthzInvitationsTokenByEmail = async function(target, data, insertQuery) {
  if (_.isEmpty(data.rows)) {
    return;
  }
  await (async function(targetClient, rows) {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      await targetClient.execute(insertQuery, [row.email, row.token], clientOptions);
    }
  })(target.client, data.rows);
};

const fetchAuthzInvitationsTokenByEmail = async function(target, query) {
  const result = await target.client.execute(
    query,
    [Store.getAttribute('allInvitationEmails')],
    clientOptions
  );

  logger.info(
    `${chalk.green(`✓`)}  Fetched ${
      result.rows.length
    } AuthzInvitationsTokenByEmail rows from ${chalk.cyan(target.database.host)}`
  );

  return result;
};

const copyAuthzInvitationsTokenByEmail = async function(source, destination) {
  if (_.isEmpty(Store.getAttribute('allInvitationEmails'))) {
    logger.info(chalk.cyan(`✗  Skipped fetching AuthzInvitationsTokenByEmail rows...\n`));
    return [];
  }
  const query = `
      SELECT * FROM "AuthzInvitationsTokenByEmail"
      WHERE email
      IN ?
      LIMIT ${clientOptions.fetchSize}`;
  const insertQuery = `
      INSERT INTO "AuthzInvitationsTokenByEmail" (
      email,
      "token")
      VALUES (?, ?)`;

  const fetchedRows = await fetchAuthzInvitationsTokenByEmail(source, query);
  Store.setAttribute('allInvitationTokens', _.uniq(_.pluck(fetchedRows.rows, 'token')));
  await insertAuthzInvitationsTokenByEmail(destination, fetchedRows, insertQuery);
};

const insertAuthzInvitationsEmailByToken = async function(target, data, insertQuery) {
  if (_.isEmpty(data.rows)) {
    return;
  }
  await (async function(targetClient, rows) {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      await targetClient.execute(insertQuery, [row.token, row.email], clientOptions);
    }
  })(target.client, data.rows);
};

const fetchAuthzInvitationsEmailByToken = async function(target, query) {
  const result = await target.client.execute(
    query,
    [Store.getAttribute('allInvitationTokens')],
    clientOptions
  );
  logger.info(
    `${chalk.green(`✓`)}  Fetched ${
      result.rows.length
    } AuthzInvitationsEmailByToken rows from ${chalk.cyan(target.database.host)}`
  );

  return result;
};

const copyAuthzInvitationsEmailByToken = async function(source, destination) {
  if (_.isEmpty(Store.getAttribute('allInvitationTokens'))) {
    logger.info(chalk.cyan(`✗  Skipped fetching AuthzInvitationsEmailByToken rows...\n`));
    return [];
  }

  const query = `
      SELECT *
      FROM "AuthzInvitationsEmailByToken"
      WHERE "token"
      IN ?
      LIMIT ${clientOptions.fetchSize}`;
  const insertQuery = `
      INSERT INTO "AuthzInvitationsEmailByToken" (
      "token",
      email)
      VALUES (?, ?)`;

  const fetchedRows = await fetchAuthzInvitationsEmailByToken(source, query);
  await insertAuthzInvitationsEmailByToken(destination, fetchedRows, insertQuery);
};

module.exports = {
  copyAuthzInvitations,
  copyAuthzInvitationsEmailByToken,
  copyAuthzInvitationsResourceIdByEmail,
  copyAuthzInvitationsTokenByEmail
};
