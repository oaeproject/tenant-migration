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

const chalk = require('chalk');
const logger = require('../logger');

let counter = 0;
const rowsToCopy = [];

const clientOptions = {
  fetchSize: 999999,
  prepare: true
};

const copyLibraryIndex = function(source, target) {
  const query = `
      SELECT *
      FROM "LibraryIndex"
      LIMIT ${clientOptions.fetchSize * 100}`;
  const insertQuery = `
      INSERT INTO "LibraryIndex" (
      "bucketKey",
      "rankedResourceId",
      value)
      VALUES (?, ?, ?)`;

  async function insertRows(targetClient, rows) {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      // eslint-disable-next-line no-await-in-loop
      await targetClient.execute(
        insertQuery,
        [row.bucketKey, row.rankedResourceId, row.value],
        clientOptions
      );
    }
  }

  const filterRows = function() {
    let row;
    while ((row = this.read())) {
      counter++;

      const tenantAlias = row.bucketKey.split(':')[2];
      if (tenantAlias === source.database.tenantAlias) {
        rowsToCopy.push(row);
      }
    }
  };

  const afterQuery = async function() {
    logger.info(`${chalk.green(`✓`)}  Filtered ${counter} LibraryIndex rows...`);
    logger.info(`${chalk.green(`✓`)}  Inserting ${rowsToCopy.length} LibraryIndex rows...`);
    await insertRows(target.client, rowsToCopy);
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

module.exports = { copyLibraryIndex };
