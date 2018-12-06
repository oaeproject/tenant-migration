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

const clientOptions = {
  fetchSize: 999999,
  prepare: true
};

const fetchGroupsFromTenant = async function(source) {
  const allRows = [];
  const query = `
      SELECT *
      FROM "AuthzMembers"
      LIMIT ${clientOptions.fetchSize}`;

  const afterQuery = function() {
    logger.info(
      `${chalk.green(`✓`)}  Fetched ${allRows.length} AuthzMembers rows from tenant ${
        source.database.tenantAlias
      }`
    );
    return allRows;
  };

  const belongsToTenant = function(id) {
    return id.split(':')[1] === source.database.tenantAlias;
  };

  const doesNotBelongToTenant = function(id) {
    return id.split(':')[1] !== source.database.tenantAlias;
  };

  const isGroup = function(id) {
    return id.startsWith('g:');
  };

  const isFolder = function(id) {
    return id.startsWith('f:');
  };

  const filterRows = function() {
    let row;
    while ((row = this.read())) {
      if (isGroup(row.resourceId)) {
        if (belongsToTenant(row.resourceId)) {
          if (doesNotBelongToTenant(row.memberId)) {
            // these are the external members of a group, so return these!
            allRows.push(row);
          }
        }
      } else if (isFolder(row.resourceId)) {
        console.log('....................................');
        if (belongsToTenant(row.resourceId)) {
          if (doesNotBelongToTenant(row.memberId)) {
            console.log('-> ' + row.resourceId + ' | ' + row.memberId);
          }
        }
      }
    }
  };

  function streamRows() {
    const com = source.client.stream(query);
    const promise = new Promise((resolve, reject) => {
      com.on('end', () => {
        resolve(afterQuery());
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

const fetchPrincipalDetails = async function(target, principalId) {
  const query = `
      SELECT *
      FROM "Principals"
      WHERE "principalId" = ?
      LIMIT ${clientOptions.fetchSize}`;

  const result = await target.client.execute(query, [principalId], clientOptions);
  /*
  logger.info(
    `${chalk.green(`✓`)}  Fetched ${result.rows.length} group row: (${chalk.cyan(groupId)})`
  );
  */
  const firstRow = _.first(result.rows);
  return firstRow;
};

module.exports = { fetchGroupsFromTenant, fetchPrincipalDetails };
