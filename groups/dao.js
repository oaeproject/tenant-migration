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
const { Store } = require("../store");
const util = require("../util");

const clientOptions = {
  fetchSize: 999999,
  prepare: true
};

const insertUserGroupVisits = async function(target, data, insertQuery) {
  if (_.isEmpty(data.rows)) {
    return;
  }

  await (async function insertAll(targetClient, rows) {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      await targetClient.execute(
        insertQuery,
        [row.userId, row.groupId, row.latestVisit],
        clientOptions
      );
    }
  })(target.client, data.rows);
};

const fetchUserGroupVisits = async function(target, query) {
  let result = await target.client.execute(
    query,
    [Store.getAttribute("tenantPrincipals")],
    clientOptions
  );

  logger.info(
    `${chalk.green(`✓`)}  Fetched ${
      result.rows.length
    } UsersGroupVisits rows from ${chalk.cyan(target.database.host)}`
  );

  return result;
};

const copyUsersGroupVisits = async function(source, destination) {
  const query = `
      SELECT *
      FROM "UsersGroupVisits"
      WHERE "userId"
      IN ?
      LIMIT ${clientOptions.fetchSize}`;
  const insertQuery = `
      INSERT INTO "UsersGroupVisits" (
      "userId",
      "groupId",
      "latestVisit")
      VALUES (?, ?, ?)`;

  let fetchedRows = await fetchUserGroupVisits(source, query);
  await insertUserGroupVisits(destination, fetchedRows, insertQuery);
  let insertedRows = await fetchUserGroupVisits(destination, query);
  util.compareResults(fetchedRows.rows.length, insertedRows.rows.length);
};

module.exports = {
  copyUsersGroupVisits
};
