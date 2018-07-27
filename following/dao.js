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
let store = require("../store");

const selectFollowingUsersFollowers = function(sourceClient) {
    let query = `SELECT * FROM "FollowingUsersFollowers" WHERE "userId" IN ?`;
    return sourceClient.execute(query, [store.tenantPrincipals]);
};

const insertFollowingUsersFollowers = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(
            `${chalk.green(`✓`)}  No FollowingUsersFollowers rows found...`
        );
        return;
    }

    let allInserts = [];
    result.rows.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "FollowingUsersFollowers" ("userId", "followerId", "value") VALUES (?, ?, ?)`,
            params: [row.userId, row.followerId, row.value]
        });
    });
    logger.info(`${chalk.green(`✓`)}  Inserting FollowingUsersFollowers...`);
    await targetClient.batch(allInserts, { prepare: true });
};

const selectFollowingUsersFollowing = function(sourceClient) {
    let query = `SELECT * FROM "FollowingUsersFollowing" WHERE "userId" IN ?`;
    return sourceClient.execute(query, [store.tenantPrincipals]);
};

const insertFollowingUsersFollowing = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(
            `${chalk.green(`✓`)}  No FollowingUsersFollowing rows found...`
        );
        return;
    }

    let allInserts = [];
    result.rows.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "FollowingUsersFollowing" ("userId", "followingId", "value") VALUES (?, ?, ?)`,
            params: [row.userId, row.followingId, row.value]
        });
    });
    logger.info(`${chalk.green(`✓`)}  Inserting FollowingUsersFollowing...`);
    await targetClient.batch(allInserts, { prepare: true });
};

module.exports = {
    selectFollowingUsersFollowers,
    selectFollowingUsersFollowing,
    insertFollowingUsersFollowers,
    insertFollowingUsersFollowing
};
