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
const store = require("../store");
const util = require("../util");

const clientOptions = {
    fetchSize: 999999,
    prepare: true
};

const copyUsersGroupVisits = async function(sourceClient, targetClient) {
    const query = `SELECT * FROM "UsersGroupVisits" WHERE "userId" IN ? LIMIT ${
        clientOptions.fetchSize
    }`;
    const insertQuery = `INSERT INTO "UsersGroupVisits" ("userId", "groupId", "latestVisit") VALUES (?, ?, ?)`;
    let counter = 0;

    let result = await sourceClient.execute(
        query,
        [store.tenantPrincipals],
        clientOptions
    );

    logger.info(
        `${chalk.green(`✓`)}  Fetched ${
            result.rows.length
        } UsersGroupVisits rows...`
    );

    if (_.isEmpty(result.rows)) {
        return;
    }

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            counter++;

            await targetClient.execute(
                insertQuery,
                [row.userId, row.groupId, row.latestVisit],
                clientOptions
            );
        }
    }
    await insertAll(targetClient, result.rows);
    logger.info(
        `${chalk.green(`✓`)}  Inserted ${counter} UsersGroupVisits rows...`
    );

    const queryResultOnSource = result;
    result = await targetClient.execute(
        query,
        [store.tenantPrincipals],
        clientOptions
    );
    util.compareBothTenants(
        queryResultOnSource.rows.length,
        result.rows.length
    );
};

module.exports = {
    copyUsersGroupVisits
};
