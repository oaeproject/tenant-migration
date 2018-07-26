const chalk = require("chalk");
const _ = require("underscore");
const logger = require("../logger");
let store = require("../store");
let sourceDatabase = store.sourceDatabase;
let targetDatabase = store.targetDatabase;

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
