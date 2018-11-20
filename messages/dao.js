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

const clientOptions = {
  fetchSize: 999999,
  prepare: true
};

const insertDiscussions = async function(target, data, insertQuery) {
  if (_.isEmpty(data.rows)) {
    return;
  }

  await (async function(targetClient, rows) {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      await targetClient.execute(
        insertQuery,
        [
          row.id,
          row.created,
          row.createdBy,
          row.description,
          row.displayName,
          row.lastModified,
          row.tenantAlias,
          row.visibility
        ],
        clientOptions
      );
    }
  })(target.client, data.rows);
};

const fetchDiscussions = async function(target, query) {
  const result = await target.client.execute(query, [target.database.tenantAlias], clientOptions);

  logger.info(
    `${chalk.green(`✓`)}  Fetched ${result.rows.length} Discussions rows from ${chalk.cyan(
      target.database.host
    )}`
  );
  return result;
};

const copyDiscussions = async function(source, destination) {
  const query = `
      SELECT *
      FROM "Discussions"
      WHERE "tenantAlias" = ?
      LIMIT ${clientOptions.fetchSize}`;
  const insertQuery = `
      INSERT INTO "Discussions" (
      id,
      created,
      "createdBy",
      description,
      "displayName",
      "lastModified",
      "tenantAlias",
      visibility)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)`;

  const fetchedRows = await fetchDiscussions(source, query);
  const discussionsFromThisTenancyAlone = _.pluck(fetchedRows.rows, 'id');
  Store.setAttribute('discussionsFromThisTenancyAlone', _.uniq(discussionsFromThisTenancyAlone));
  await insertDiscussions(destination, fetchedRows, insertQuery);
};

const copyMessageBoxMessages = function(source, destination) {
  if (_.isEmpty(Store.getAttribute('allResourceIds'))) {
    logger.info(chalk.cyan(`✗  Skipped fetching MessageBoxMessages rows...\n`));
    return [];
  }

  const query = `
      SELECT *
      FROM "MessageBoxMessages"
      WHERE "messageBoxId"
      IN ?
      LIMIT ${clientOptions.fetchSize}`;
  const insertQuery = `
      INSERT INTO "MessageBoxMessages" (
      "messageBoxId",
      "threadKey",
      value)
      VALUES (?, ?, ?)`;
  const allRows = [];
  const threadKeysFromThisTenancyAlone = [];

  const afterQuery = async function() {
    Store.setAttribute('threadKeysFromThisTenancyAlone', _.uniq(threadKeysFromThisTenancyAlone));
    logger.info(`${chalk.green(`✓`)}  Fetched ${allRows.length} MessageBoxMessages rows`);

    if (_.isEmpty(allRows)) {
      return;
    }
    await insertRows(destination.client, allRows);
  };

  const insertRows = async function(target, rows) {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      await target.execute(
        insertQuery,
        [row.messageBoxId, row.threadKey, row.value],
        clientOptions
      );
    }
  };

  const filterRows = function() {
    let row;
    while ((row = this.read())) {
      if (row.threadKey) {
        allRows.push(row);
        threadKeysFromThisTenancyAlone.push({
          messageBoxId: row.messageBoxId,
          threadKey: _.last(row.threadKey.split('#'))
        });
      }
    }
  };

  function streamRows() {
    const com = source.client.stream(query, [
      _.union(
        Store.getAttribute('allContentIds'),
        Store.getAttribute('discussionsFromThisTenancyAlone'),
        Store.getAttribute('allResourceIds')
      )
    ]);
    const promise = new Promise((resolve, reject) => {
      com.on('end', async () => {
        await afterQuery();
        resolve();
      });
      com.on('error', reject);
    });
    promise.on = function(...args) {
      com.on(...args);
      return promise;
    };
    return promise;
  }
  return streamRows().on('readable', filterRows);
};

const copyMessages = function(source, destination) {
  if (_.isEmpty(Store.getAttribute('allResourceIds'))) {
    logger.info(chalk.cyan(`✗  Skipped fetching Messages rows...\n`));
    return [];
  }

  const query = `
      SELECT *
      FROM "Messages"
      LIMIT ${clientOptions.fetchSize}`;
  const insertQuery = `
      INSERT INTO "Messages" (
      id,
      body,
      "createdBy",
      deleted,
      "threadKey")
      VALUES (?, ?, ?, ?, ?)`;
  const allRows = [];
  const allTenantMessages = [];

  const afterQuery = async function() {
    Store.setAttribute('allTenantMessages', _.uniq(allTenantMessages));
    logger.info(`${chalk.green(`✓`)}  Fetched ${allRows.length} Messages rows`);

    if (_.isEmpty(allRows)) {
      return;
    }
    await insertRows(destination.client, allRows);
  };

  const filterRows = function() {
    let row;
    while ((row = this.read())) {
      if (row.id && row.id.split(':')[1] === source.database.tenantAlias) {
        allRows.push(row);
        allTenantMessages.push(row.id);
      }
    }
  };

  const insertRows = async function(target, rows) {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      await target.execute(
        insertQuery,
        [row.id, row.body, row.createdBy, row.deleted, row.threadKey],
        clientOptions
      );
    }
  };

  function streamRows() {
    const com = source.client.stream(query);
    const p = new Promise((resolve, reject) => {
      com.on('end', async () => {
        await afterQuery();
        resolve();
      });
      com.on('error', reject);
    });
    p.on = function(...args) {
      com.on(...args);
      return p;
    };
    return p;
  }
  return streamRows().on('readable', filterRows);
};

const insertMessageBoxMessagesDeleted = async function(target, data, insertQuery) {
  if (_.isEmpty(data.rows)) {
    return;
  }
  await (async function(targetClient, rows) {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      await targetClient.execute(
        insertQuery,
        [row.messageBoxId, row.createdTimestamp, row.value],
        clientOptions
      );
    }
  })(target.client, data.rows);
};

const fetchMessageBoxMessagesDeleted = async function(target, query) {
  const discussionsAndContentIds = _.union(
    Store.getAttribute('discussionsFromThisTenancyAlone'),
    Store.getAttribute('allContentIds'),
    Store.getAttribute('allResourceIds')
  );

  const result = await target.client.execute(query, [discussionsAndContentIds], clientOptions);

  logger.info(
    `${chalk.green(`✓`)}  Fetched ${
      result.rows.length
    } MessageBoxMessagesDeleted rows from ${chalk.cyan(target.database.host)}`
  );
  return result;
};

const copyMessageBoxMessagesDeleted = async function(source, destination) {
  // If (_.isEmpty(Store.getAttribute("discussionsFromThisTenancyAlone"))) {
  if (_.isEmpty(Store.getAttribute('allResourceIds'))) {
    logger.info(chalk.cyan(`✗  Skipped fetching MessageBoxMessagesDeleted rows...\n`));
    return [];
  }
  const query = `
      SELECT *
      FROM "MessageBoxMessagesDeleted"
      WHERE "messageBoxId"
      IN ?
      LIMIT ${clientOptions.fetchSize}`;
  const insertQuery = `
      INSERT INTO "MessageBoxMessagesDeleted" (
      "messageBoxId",
      "createdTimestamp",
      value)
      VALUES (?, ? ,?)`;

  const fetchedRows = await fetchMessageBoxMessagesDeleted(source, query);
  await insertMessageBoxMessagesDeleted(destination, fetchedRows, insertQuery);
};

const insertMessageBoxRecentContributions = async function(target, data, insertQuery) {
  if (_.isEmpty(data.rows)) {
    return;
  }
  await (async function(targetClient, rows) {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      await targetClient.execute(
        insertQuery,
        [row.messageBoxId, row.contributorId, row.value],
        clientOptions
      );
    }
  })(target.client, data.rows);
};

const fetchMessageBoxRecentContributions = async function(target, query) {
  const discussionsAndContentIds = _.union(
    Store.getAttribute('allContentIds'),
    Store.getAttribute('discussionsFromThisTenancyAlone'),
    Store.getAttribute('allResourceIds')
  );

  const result = await target.client.execute(query, [discussionsAndContentIds], clientOptions);

  logger.info(
    `${chalk.green(`✓`)}  Fetched ${
      result.rows.length
    } MessageBoxRecentContributions rows from ${chalk.cyan(target.database.host)}`
  );
  return result;
};

const copyMessageBoxRecentContributions = async function(source, destination) {
  if (_.isEmpty(Store.getAttribute('allResourceIds'))) {
    logger.info(chalk.cyan(`✗  Skipped fetching MessageBoxRecentContributions rows...\n`));
    return [];
  }
  // MessageBoxRecentContributions
  const query = `
      SELECT *
      FROM "MessageBoxRecentContributions"
      WHERE "messageBoxId"
      IN ?
      LIMIT ${clientOptions.fetchSize}`;
  const insertQuery = `
      INSERT INTO "MessageBoxRecentContributions" (
      "messageBoxId",
      "contributorId",
      value)
      VALUES (?, ?, ?)`;

  const fetchedRows = await fetchMessageBoxRecentContributions(source, query);
  await insertMessageBoxRecentContributions(destination, fetchedRows, insertQuery);
};

module.exports = {
  copyMessages,
  copyDiscussions,
  copyMessageBoxMessages,
  copyMessageBoxMessagesDeleted,
  copyMessageBoxRecentContributions
};
