const chalk = require("chalk");
const _ = require("underscore");
const logger = require("../logger");
let store = require("../store");
let sourceDatabase = store.sourceDatabase;
let targetDatabase = store.targetDatabase;

const selectUsersGroupVisits = function(sourceClient) {
    let query = `SELECT * FROM "UsersGroupVisits" WHERE "userId" IN ?`;
    return sourceClient.execute(query, [store.tenantPrincipals]);
};

const insertUsersGroupVisits = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(`${chalk.green(`✓`)}  No UsersGroupVisits rows found...`);

        return;
    }

    let allInserts = [];
    result.rows.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "UsersGroupVisits" ("userId", "groupId", "latestVisit") VALUES (?, ?, ?)`,
            params: [row.userId, row.groupId, row.latestVisit]
        });
    });
    logger.info(`${chalk.green(`✓`)}  Inserting UsersGroupVisits...`);
    await targetClient.batch(allInserts, { prepare: true });
};

module.exports = {
    selectUsersGroupVisits,
    insertUsersGroupVisits
};
