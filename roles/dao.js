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

/* eslint-disable no-await-in-loop */
const chalk = require('chalk');
const _ = require('underscore');
const logger = require('../logger');
const util = require('../util');
const { Store } = require('../store');

const clientOptions = {
  fetchSize: 999999,
  prepare: true
};

const insertAuthzRoles = async function(target, data, insertQuery) {
  if (_.isEmpty(data.rows)) {
    return;
  }
  await (async function(targetClient, rows) {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      await targetClient.execute(
        insertQuery,
        [row.principalId, row.resourceId, row.role],
        clientOptions
      );
    }
  })(target.client, data.rows);
};

const fetchAuthzRoles = async function(target, query) {
  const result = await target.client.execute(
    query,
    [Store.getAttribute('tenantPrincipals')],
    clientOptions
  );
  logger.info(
    `${chalk.green(`✓`)}  Fetched ${result.rows.length} AuthzRoles rows from ${chalk.cyan(
      target.database.host
    )}`
  );
  return result;
};

const copyAuthzRoles = async function(source, destination) {
  const _isManager = role => {
    return role === 'manager';
  };

  const _belongsToOtherTenant = resourceId => {
    return resourceId.split(':')[1] !== source.database.tenantAlias;
  };

  const query = `
      SELECT *
      FROM "AuthzRoles"
      WHERE "principalId"
      IN ?
      LIMIT ${clientOptions.fetchSize}`;
  const insertQuery = `
      INSERT INTO "AuthzRoles" (
      "principalId",
      "resourceId",
      role)
      VALUES (?, ?, ?)`;

  const fetchedRows = await fetchAuthzRoles(source, query);
  // Some resources from other tenants must be copied too
  // in case at least one of the managers belongs to the tenant being moved
  const movedResources = [];
  _.each(fetchedRows.rows, eachRow => {
    if (_belongsToOtherTenant(eachRow.resourceId) && _isManager(eachRow.role)) {
      const oldResourceId = eachRow.resourceId;
      eachRow.resourceId = [
        eachRow.resourceId.split(':')[0],
        source.database.tenantAlias,
        eachRow.resourceId.split(':')[2]
      ].join(':');

      // I need to store these so that I can later copy the correspondent files
      movedResources.push(oldResourceId);
    }
  });
  Store.setAttribute('movedResources', movedResources);

  // Lets move the tenancy of this resource in case it's a manager
  const allResources = _.filter(fetchedRows.rows, eachResource => {
    return eachResource.resourceId.split(':')[1] === source.database.tenantAlias;
  });
  let allResourceIds = _.pluck(allResources, 'resourceId');
  Store.setAttribute('allResourceIds', _.uniq(allResourceIds));
  await insertAuthzRoles(destination, fetchedRows, insertQuery);

  const insertedRows = await fetchAuthzRoles(destination, query);
  util.compareResults(fetchedRows.rows.length, insertedRows.rows.length);
};

const insertAuthzMembers = async function(target, data, insertQuery) {
  if (_.isEmpty(data.rows)) {
    return;
  }
  await (async function(targetClient, rows) {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      await targetClient.execute(
        insertQuery,
        [row.resourceId, row.memberId, row.role],
        clientOptions
      );
    }
  })(target.client, data.rows);
};

const fetchAuthzMembers = async function(target, query) {
  const result = await target.client.execute(
    query,
    [Store.getAttribute('tenantPrincipals')],
    clientOptions
  );
  logger.info(
    `${chalk.green(`✓`)}  Fetched ${result.rows.length} AuthzMembers rows from ${chalk.cyan(
      target.database.host
    )}`
  );

  return result;
};

const copyAuthzMembers = async function(source, destination) {
  const query = `
      SELECT *
      FROM "AuthzMembers"
      WHERE "memberId"
      IN ?
      LIMIT ${clientOptions.fetchSize} ALLOW FILTERING`;
  const insertQuery = `
      INSERT INTO "AuthzMembers" (
      "resourceId",
      "memberId",
      role)
      VALUES (?, ?, ?)`;

  const fetchedRows = await fetchAuthzMembers(source, query);
  await insertAuthzMembers(destination, fetchedRows, insertQuery);
  const insertedRows = await fetchAuthzMembers(destination, query);
  util.compareResults(fetchedRows.rows.length, insertedRows.rows.length);
};

module.exports = {
  copyAuthzMembers,
  copyAuthzRoles
};
