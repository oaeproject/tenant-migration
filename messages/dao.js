const chalk = require("chalk");
const _ = require("underscore");
const logger = require("../logger");
let store = require("../store");
let sourceDatabase = store.sourceDatabase;
let targetDatabase = store.targetDatabase;

const selectDiscussions = function(sourceClient) {
    let allRows = [];
    // lets query discussions and all messages
    function doAllTheThings() {
        let query = `SELECT * FROM "Discussions"`;
        var com = sourceClient.stream(query);
        var p = new Promise(function(resolve, reject) {
            com.on("end", function() {
                resolve(allRows);
            });
            com.on("error", reject);
        });
        p.on = function() {
            com.on.apply(com, arguments);
            return p;
        };
        return p;
    }

    // query "Discussions" - This is very very inadequate but we can't filter it!

    return doAllTheThings().on("readable", function() {
        // 'readable' is emitted as soon a row is received and parsed
        let row;
        while ((row = this.read())) {
            if (
                row.tenantAlias &&
                row.tenantAlias === sourceDatabase.tenantAlias
            ) {
                allRows.push(row);
                store.discussionsFromThisTenancyAlone.push(row.id);
            }
        }
    });
};

const insertDiscussions = async function(targetClient, result) {
    if (_.isEmpty(result)) {
        logger.info(`${chalk.green(`✓`)}  No Discussions rows found...`);

        return;
    }

    let allInserts = [];
    result.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "Discussions" (id, created, "createdBy", description, "displayName", "lastModified", "tenantAlias", visibility) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
            params: [
                row.id,
                row.created,
                row.createdBy,
                row.description,
                row.displayName,
                row.lastModified,
                row.tenantAlias,
                row.visibility
            ]
        });
    });
    logger.info(`${chalk.green(`✓`)}  Inserting Discussions...`);
    await targetClient.batch(allInserts, { prepare: true });
};

const selectMessageBoxMessages = function(sourceClient) {
    let allRows = [];
    if (_.isEmpty(store.discussionsFromThisTenancyAlone)) {
        return [];
    }
    // lets query discussions and all messages
    function doAllTheThings() {
        let query = `SELECT * FROM "MessageBoxMessages" WHERE "messageBoxId" IN ?`;
        var com = sourceClient.stream(query, [
            store.discussionsFromThisTenancyAlone
        ]);
        var p = new Promise(function(resolve, reject) {
            com.on("end", function() {
                resolve(allRows);
            });
            com.on("error", reject);
        });
        p.on = function() {
            com.on.apply(com, arguments);
            return p;
        };
        return p;
    }

    return doAllTheThings().on("readable", function() {
        // 'readable' is emitted as soon a row is received and parsed
        let row;
        while ((row = this.read())) {
            if (row.threadKey) {
                allRows.push(row);
                store.threadKeysFromThisTenancyAlone.push({
                    messageBoxId: row.messageBoxId,
                    threadKey: _.last(row.threadKey.split("#"))
                });
            }
        }
    });
};

const insertMessageBoxMessages = async function(targetClient, result) {
    if (_.isEmpty(result)) {
        logger.info(`${chalk.green(`✓`)}  No MessageBoxMessages rows found...`);

        return;
    }

    let allInserts = [];
    result.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "MessageBoxMessages" ("messageBoxId", "threadKey", value) VALUES (?, ?, ?)`,
            params: [row.messageBoxId, row.threadKey, row.value]
        });
    });
    logger.info(`${chalk.green(`✓`)}  Inserting MessageBoxMessages...`);
    await targetClient.batch(allInserts, { prepare: true });
};

const selectMessages = function(sourceClient) {
    let allRows = [];
    if (_.isEmpty(store.discussionsFromThisTenancyAlone)) {
        return [];
    }
    // lets query discussions and all messages
    function doAllTheThings() {
        let query = `SELECT * FROM "Messages"`;
        var com = sourceClient.stream(query);
        var p = new Promise(function(resolve, reject) {
            com.on("end", function() {
                resolve(allRows);
            });
            com.on("error", reject);
        });
        p.on = function() {
            com.on.apply(com, arguments);
            return p;
        };
        return p;
    }

    return doAllTheThings().on("readable", function() {
        // 'readable' is emitted as soon a row is received and parsed
        let row;
        while ((row = this.read())) {
            if (row.id && row.id.split(":")[1] === sourceDatabase.tenantAlias) {
                allRows.push(row);
                store.allTenantMessages.push(row.id);
            }
        }
    });
};

const insertMessages = async function(targetClient, result) {
    if (_.isEmpty(result)) {
        logger.info(`${chalk.green(`✓`)}  No Messages rows found...`);

        return;
    }

    let allInserts = [];
    result.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "Messages" (id, body, "createdBy", deleted, "threadKey") VALUES (?, ?, ?, ?, ?)`,
            params: [
                row.id,
                row.body,
                row.createdBy,
                row.deleted,
                row.threadKey
            ]
        });
    });
    logger.info(`${chalk.green(`✓`)}  Inserting Messages...`);
    await targetClient.batch(allInserts, { prepare: true });
};

const selectMessageBoxMessagesDeleted = function(sourceClient) {
    if (_.isEmpty(store.discussionsFromThisTenancyAlone)) {
        return [];
    }
    // MessageBoxMessagesDeleted
    let query = `SELECT * FROM "MessageBoxMessagesDeleted" WHERE "messageBoxId" IN ?`;
    return sourceClient.execute(query, [store.discussionsFromThisTenancyAlone]);
};

const insertMessageBoxMessagesDeleted = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(
            `${chalk.green(`✓`)}  No MessageBoxMessagesDeleted rows found...`
        );

        return;
    }

    let allInserts = [];
    result.rows.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "MessageBoxMessagesDeleted" ("messageBoxId", "createdTimestamp", value) VALUES (?, ? ,?)`,
            params: [row.messageBoxId, row.createdTimestamp, row.value]
        });
    });
    logger.info(`${chalk.green(`✓`)}  Inserting MessageBoxMessagesDeleted...`);
    await targetClient.batch(allInserts, { prepare: true });
};

const selectMessageBoxRecentContributions = function(sourceClient) {
    if (_.isEmpty(store.discussionsFromThisTenancyAlone)) {
        return [];
    }
    // MessageBoxRecentContributions
    let query = `SELECT * FROM "MessageBoxRecentContributions" WHERE "messageBoxId" IN ?`;
    return sourceClient.execute(query, [store.discussionsFromThisTenancyAlone]);
};

const insertMessageBoxRecentContributions = async function(
    targetClient,
    result
) {
    if (_.isEmpty(result.rows)) {
        logger.info(
            `${chalk.green(
                `✓`
            )}  No MessageBoxRecentContributions rows found...`
        );

        return;
    }

    let allInserts = [];
    result.rows.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "MessageBoxRecentContributions" ("messageBoxId", "contributorId", value) VALUES (?, ?, ?)`,
            params: [row.messageBoxId, row.contributorId, row.value]
        });
    });
    logger.info(
        `${chalk.green(`✓`)}  Inserting MessageBoxRecentContributions...`
    );
    await targetClient.batch(allInserts, { prepare: true });
};

module.exports = {
    insertDiscussions,
    insertMessageBoxMessages,
    insertMessageBoxMessagesDeleted,
    insertMessageBoxRecentContributions,
    insertMessages,
    selectDiscussions,
    selectMessageBoxMessages,
    selectMessageBoxMessagesDeleted,
    selectMessageBoxRecentContributions,
    selectMessages
};
