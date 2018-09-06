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

const copyFollowingUsersFollowers = async function(source, target) {
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

    let result = await source.client.execute(
        query,
        [Store.getAttribute("tenantPrincipals")],
        clientOptions
    );

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];

            await targetClient.execute(
                insertQuery,
                [row.userId, row.followerId, row.value],
                clientOptions
            );
        }
    }

    logger.info(
        `${chalk.green(`✓`)}  Fetched ${
            result.rows.length
        } FollowingUsersFollowers rows...`
    );
    if (_.isEmpty(result.rows)) {
        return;
    }
    await insertAll(target.client, result.rows);

    const queryResultOnSource = result;
    result = await target.client.execute(
        query,
        [Store.getAttribute("tenantPrincipals")],
        clientOptions
    );
    util.compareResults(queryResultOnSource.rows.length, result.rows.length);
};

const copyFollowingUsersFollowing = async function(source, target) {
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

    let result = await source.client.execute(
        query,
        [Store.getAttribute("tenantPrincipals")],
        clientOptions
    );

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];

            await targetClient.execute(
                insertQuery,
                [row.userId, row.followingId, row.value],
                clientOptions
            );
        }
    }

    logger.info(
        `${chalk.green(`✓`)}  Fetched ${
            result.rows.length
        } FollowingUsersFollowing rows...`
    );
    if (_.isEmpty(result.rows)) {
        return;
    }
    await insertAll(target.client, result.rows);

    const queryResultOnSource = result;
    result = await target.client.execute(
        query,
        [Store.getAttribute("tenantPrincipals")],
        clientOptions
    );
    util.compareResults(queryResultOnSource.rows.length, result.rows.length);
};

module.exports = {
    copyFollowingUsersFollowers,
    copyFollowingUsersFollowing
};
