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

const insertAllFollowers = async function(target, data, insertQuery) {
  if (_.isEmpty(data.rows)) {
    return;
  }
  await (async function(targetClient, rows) {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      await targetClient.execute(
        insertQuery,
        [row.userId, row.followerId, row.value],
        clientOptions
      );
    }
  })(target.client, data.rows);
};

const fetchAllFollowers = async function(target, query) {
  const result = await target.client.execute(
    query,
    [Store.getAttribute('tenantPrincipals')],
    clientOptions
  );

  logger.info(
    `${chalk.green(`✓`)}  Fetched ${
      result.rows.length
    } FollowingUsersFollowers rows from ${chalk.cyan(target.database.host)}`
  );

  return result;
};

const copyFollowingUsersFollowers = async function(source, destination) {
  const query = `
      SELECT *
      FROM "FollowingUsersFollowers"
      WHERE "userId"
      IN ?
      LIMIT ${clientOptions.fetchSize}`;
  const insertQuery = `
      INSERT INTO "FollowingUsersFollowers" (
      "userId",
      "followerId",
      "value")
      VALUES (?, ?, ?)`;

  const fetchedRows = await fetchAllFollowers(source, query);
  await insertAllFollowers(destination, fetchedRows, insertQuery);
  const insertedRows = await fetchAllFollowers(destination, query);
  util.compareResults(fetchedRows.rows.length, insertedRows.rows.length);
};

const insertAllFollowing = async function(target, data, insertQuery) {
  if (_.isEmpty(data.rows)) {
    return;
  }
  await (async function(targetClient, rows) {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      await targetClient.execute(
        insertQuery,
        [row.userId, row.followingId, row.value],
        clientOptions
      );
    }
  })(target.client, data.rows);
};

const fetchAllFollowing = async function(target, query) {
  const result = await target.client.execute(
    query,
    [Store.getAttribute('tenantPrincipals')],
    clientOptions
  );
  logger.info(
    `${chalk.green(`✓`)}  Fetched ${
      result.rows.length
    } FollowingUsersFollowing rows from ${chalk.cyan(target.database.host)}`
  );
  return result;
};

const copyFollowingUsersFollowing = async function(source, destination) {
  const query = `
      SELECT *
      FROM "FollowingUsersFollowing"
      WHERE "userId"
      IN ?
      LIMIT ${clientOptions.fetchSize}`;
  const insertQuery = `
      INSERT INTO "FollowingUsersFollowing" (
      "userId",
      "followingId",
      "value")
      VALUES (?, ?, ?)`;

  const fetchedRows = await fetchAllFollowing(source, query);
  await insertAllFollowing(destination, fetchedRows, insertQuery);
  const insertedRows = await fetchAllFollowing(destination, query);
  util.compareResults(fetchedRows.rows.length, insertedRows.rows.length);
};

module.exports = {
  copyFollowingUsersFollowers,
  copyFollowingUsersFollowing
};
