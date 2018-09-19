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

const fetchAllPrincipals = async function(target, query) {
  let result = await target.client.execute(
    query,
    [target.database.tenantAlias],
    clientOptions
  );
  logger.info(
    `${chalk.green(`✓`)}  Fetched ${result.rows.length} Principals rows...`
  );

  // We'll need to know which principals are users or groups
  let tenantPrincipals = [];
  let tenantGroups = [];
  let tenantUsers = [];
  result.rows.forEach(row => {
    tenantPrincipals.push(row.principalId);
    if (row.principalId.startsWith("g")) {
      tenantGroups.push(row.principalId);
    } else if (row.principalId.startsWith("u")) {
      tenantUsers.push(row.principalId);
    }
  });
  Store.setAttribute("tenantPrincipals", _.uniq(tenantPrincipals));
  Store.setAttribute("tenantGroups", _.uniq(tenantGroups));
  Store.setAttribute("tenantUsers", _.uniq(tenantUsers));

  return result;
};

const insertPrincipals = async function(target, data, insertQuery) {
  if (_.isEmpty(data.rows)) {
    return;
  }
  await (async function insertAll(targetClient, rows) {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

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
  })(target.client, data.rows);
};

const copyPrincipals = async function(source, destination) {
  const query = `
      SELECT *
      FROM "Principals"
      WHERE "tenantAlias" = ?
      LIMIT ${clientOptions.fetchSize}`;
  const insertQuery = `
      INSERT INTO "Principals" (
      "principalId",
      "acceptedTC",
      "admin:global",
      "admin:tenant",
      created,
      "createdBy",
      deleted,
      description,
      "displayName",
      email,
      "emailPreference",
      joinable,
      "largePictureUri",
      "lastModified",
      locale,
      "mediumPictureUri",
      "notificationsLastRead",
      "notificationsUnread",
      "publicAlias",
      "smallPictureUri",
      "tenantAlias",
      visibility)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

    let fetchedRows = await fetchAllPrincipals(source, query);
    await insertPrincipals(destination, fetchedRows, insertQuery);
    let insertedRows = await fetchAllPrincipals(destination, query);
    util.compareResults(fetchedRows.rows.length, insertedRows.rows.length);
};

const fetchPrincipalsByEmail = async function(target, query) {
  let result = await target.client.execute(
    query,
    [Store.getAttribute("tenantPrincipals")],
    clientOptions
  );
  logger.info(
    `${chalk.green(`✓`)}  Fetched ${
      result.rows.length
    } PrincipalsByEmail rows...`
  );

  return result;
};

const insertPrincipalsByEmail = async function(target, data, insertQuery) {
  if (_.isEmpty(data.rows)) {
    return;
  }
  await (async function insertAll(targetClient, rows) {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      await targetClient.execute(
        insertQuery,
        [row.email, row.principalId],
        clientOptions
      );
    }
  })(target.client, data.rows);
};

const copyPrincipalsByEmail = async function(source, destination) {
  const query = `
      SELECT *
      FROM "PrincipalsByEmail"
      WHERE "principalId" IN ?
      LIMIT ${clientOptions.fetchSize}
      ALLOW FILTERING`;
  const insertQuery = `
      INSERT INTO "PrincipalsByEmail" (
        email,
        "principalId")
      VALUES (?, ?)`;

    let fetchedRows = await fetchPrincipalsByEmail(source, query);
    await insertPrincipalsByEmail(destination, fetchedRows, insertQuery);
    let insertedRows = await fetchPrincipalsByEmail(destination, query);
    util.compareResults(fetchedRows.rows.length, insertedRows.rows.length);
};

module.exports = {
  copyPrincipals,
  copyPrincipalsByEmail
};
