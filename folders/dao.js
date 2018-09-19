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

const insertAllFolders = async function(target, data, insertQuery) {
  if (_.isEmpty(data.rows)) {
    return;
  }
  await (async function insertAll(targetClient, rows) {
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
          row.groupId,
          row.lastModified,
          row.previews,
          row.tenantAlias,
          row.visibility
        ],
        clientOptions
      );
    }
  })(target.client, data.rows);
};

const fetchAllFolders = async function(target, query) {
  let result = await target.client.execute(
    query,
    [target.database.tenantAlias],
    clientOptions
  );
  let folderGroupIdsFromThisTenancyAlone = _.pluck(result.rows, "groupId");
  Store.setAttribute(
    "folderGroupIdsFromThisTenancyAlone",
    folderGroupIdsFromThisTenancyAlone
  );
  logger.info(
    `${chalk.green(`✓`)}  Fetched ${result.rows.length} Folders rows...`
  );
  return result;
};

const copyFolders = async function(source, destination) {
  const query = `
      SELECT *
      FROM "Folders"
      WHERE "tenantAlias" = ?
      LIMIT ${clientOptions.fetchSize}`;
  const insertQuery = `
      INSERT INTO "Folders" (
          id,
          created,
          "createdBy",
          description,
          "displayName",
          "groupId",
          "lastModified",
          previews,
          "tenantAlias",
          visibility)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ? )`;

    let fetchedRows = await fetchAllFolders(source, query);
    await insertAllFolders(destination, fetchedRows, insertQuery);
    let insertedRows = await fetchAllFolders(destination, query);
    util.compareResults(fetchedRows.rows.length, insertedRows.rows.length);
};

const insertFoldersGroupIds = async function(target, data, insertQuery) {
  if (_.isEmpty(data.rows)) {
    return;
  }
  await (async function insertAll(targetClient, rows) {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      await targetClient.execute(
        insertQuery,
        [row.groupId, row.folderId],
        clientOptions
      );
    }
  })(target.client, data.rows);
};

const fetchFoldersGroupIds = async function(target, query) {
  let result = await target.client.execute(
    query,
    [Store.getAttribute("folderGroupIdsFromThisTenancyAlone")],
    clientOptions
  );
  logger.info(
    `${chalk.green(`✓`)}  Fetched ${result.rows.length} FoldersGroupId rows...`
  );
  return result;
};

const copyFoldersGroupIds = async function(source, destination) {
  if (_.isEmpty(Store.getAttribute("folderGroupIdsFromThisTenancyAlone"))) {
    logger.info(chalk.cyan(`✗  Skipped fetching FoldersGroupId rows...\n`));
    return [];
  }
  const query = `
      SELECT *
      FROM "FoldersGroupId"
      WHERE "groupId"
      IN ? LIMIT ${clientOptions.fetchSize}`;
  const insertQuery = `
      INSERT INTO "FoldersGroupId" (
          "groupId",
          "folderId")
          VALUES (?, ?)`;

    let fetchedRows = await fetchFoldersGroupIds(source, query);
    await insertFoldersGroupIds(destination, fetchedRows, insertQuery);
    let insertedRows = await fetchFoldersGroupIds(destination, query);
    util.compareResults(fetchedRows.rows.length, insertedRows.rows.length);
};

module.exports = {
  copyFolders,
  copyFoldersGroupIds
};
