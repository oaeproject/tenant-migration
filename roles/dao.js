const chalk = require("chalk");
const _ = require("underscore");
const logger = require("../logger");
let store = require("../store");
let sourceDatabase = store.sourceDatabase;
let targetDatabase = store.targetDatabase;

const selectAuthzRoles = function(sourceClient) {
    let query = `SELECT * FROM "AuthzRoles" WHERE "principalId" IN ?`;
    return sourceClient.execute(query, [store.tenantPrincipals]);
};

const insertAuthzRoles = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(`${chalk.green(`✓`)}  No AuthzRoles rows found...`);

        return;
    }

    store.allResourceIds = _.pluck(result.rows, "resourceId");
    let allInserts = [];
    result.rows.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "AuthzRoles" ("principalId", "resourceId", role) VALUES (?, ?, ?)`,
            params: [row.principalId, row.resourceId, row.role]
        });
    });
    logger.info(`${chalk.green(`✓`)}  Inserting AuthzRoles...`);
    await targetClient.batch(allInserts, { prepare: true });
};

const selectAuthzMembers = function(sourceClient) {
    let query = `SELECT * FROM "AuthzMembers" WHERE "memberId" IN ? ALLOW FILTERING`;
    return sourceClient.execute(query, [store.tenantPrincipals]);
};

const insertAllAuthzMembers = async function(targetClient, result) {
    // insert authzmembers
    if (_.isEmpty(result.rows)) {
        logger.info(`${chalk.green(`✓`)}  No AuthzMembers rows found...`);
        return;
    }

    let allInserts = [];
    result.rows.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "AuthzMembers" ("resourceId", "memberId", role) VALUES (?, ?, ?)`,
            params: [row.resourceId, row.memberId, row.role]
        });
    });
    logger.info(`${chalk.green(`✓`)}  Inserting AuthzMembers...`);
    await targetClient.batch(allInserts, { prepare: true });
};

module.exports = {
    selectAuthzMembers,
    insertAllAuthzMembers,
    selectAuthzRoles,
    insertAuthzRoles
};
