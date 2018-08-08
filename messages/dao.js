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
let sourceDatabase = store.sourceDatabase;
const util = require("../util");

const clientOptions = {
    fetchSize: 999999,
    prepare: true
};

const copyDiscussions = async function(sourceClient, targetClient) {
    let query = `SELECT * FROM "Discussions"`;
    let insertQuery = `INSERT INTO "Discussions" (id, created, "createdBy", description, "displayName", "lastModified", "tenantAlias", visibility) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`;
    let counter = 0;
    let allRows = [];

    // lets query discussions and all messages
    function doAllTheThings() {
        var com = sourceClient.stream(query);
        var p = new Promise(function(resolve, reject) {
            com.on("end", async function() {
                logger.info(
                    `${chalk.green(`✓`)}  Fetched ${
                        allRows.length
                    } Discussions rows...`
                );

                if (_.isEmpty(allRows)) {
                    return;
                }
                await insertAll(targetClient, allRows);

                logger.info(
                    `${chalk.green(
                        `✓`
                    )}  Inserted ${counter} Discussions rows...`
                );

                util.compareBothTenants(allRows.length, counter);
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
    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            let row = rows[i];
            counter++;

            await targetClient.execute(
                insertQuery,
                [
                    row.id,
                    row.created,
                    row.createdBy,
                    row.description,
                    row.displayName,
                    row.lastModified,
                    row.tenantAlias,
                    row.visibility
                ],
                clientOptions
            );
        }
    }

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

const copyMessageBoxMessages = async function(sourceClient, targetClient) {
    let query = `SELECT * FROM "MessageBoxMessages" WHERE "messageBoxId" IN ?`;
    let insertQuery = `INSERT INTO "MessageBoxMessages" ("messageBoxId", "threadKey", value) VALUES (?, ?, ?)`;
    let counter = 0;
    let allRows = [];

    if (_.isEmpty(store.discussionsFromThisTenancyAlone)) {
        logger.info(
            chalk.cyan(`✗  Skipped fetching MessageBoxMessages rows...\n`)
        );
        return [];
    }

    // lets query discussions and all messages
    function doAllTheThings() {
        var com = sourceClient.stream(query, [
            _.union(store.allContentIds, store.discussionsFromThisTenancyAlone)
        ]);
        var p = new Promise(function(resolve, reject) {
            com.on("end", async function() {
                logger.info(
                    `${chalk.green(`✓`)}  Fetched ${
                        allRows.length
                    } MessageBoxMessages rows...`
                );
                if (_.isEmpty(allRows)) {
                    return;
                }
                await insertAll(targetClient, allRows);
                logger.info(
                    `${chalk.green(
                        `✓`
                    )}  Inserted ${counter} MessageBoxMessages rows...`
                );

                util.compareBothTenants(allRows.length, counter);
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

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            let row = rows[i];
            counter++;

            await targetClient.execute(
                insertQuery,
                [row.messageBoxId, row.threadKey, row.value],
                clientOptions
            );
        }
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

const copyMessages = async function(sourceClient, targetClient) {
    let query = `SELECT * FROM "Messages"`;
    let insertQuery = `INSERT INTO "Messages" (id, body, "createdBy", deleted, "threadKey") VALUES (?, ?, ?, ?, ?)`;
    let counter = 0;
    let allRows = [];

    if (_.isEmpty(store.discussionsFromThisTenancyAlone)) {
        logger.info(chalk.cyan(`✗  Skipped fetching Messages rows...\n`));
        return [];
    }
    // lets query discussions and all messages
    function doAllTheThings() {
        var com = sourceClient.stream(query);
        var p = new Promise(function(resolve, reject) {
            com.on("end", async function() {
                logger.info(
                    `${chalk.green(`✓`)}  Fetched ${
                        allRows.length
                    } Messages rows...`
                );

                if (_.isEmpty(allRows)) {
                    return;
                }
                await insertAll(targetClient, allRows);
                logger.info(
                    `${chalk.green(`✓`)}  Inserted ${counter} Messages rows...`
                );

                util.compareBothTenants(allRows.length, counter);
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

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            let row = rows[i];
            counter++;

            await targetClient.execute(
                insertQuery,
                [row.id, row.body, row.createdBy, row.deleted, row.threadKey],
                clientOptions
            );
        }
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

const copyMessageBoxMessagesDeleted = async function(
    sourceClient,
    targetClient
) {
    if (_.isEmpty(store.discussionsFromThisTenancyAlone)) {
        logger.info(
            chalk.cyan(
                `✗  Skipped fetching MessageBoxMessagesDeleted rows...\n`
            )
        );
        return [];
    }
    // MessageBoxMessagesDeleted
    let query = `SELECT * FROM "MessageBoxMessagesDeleted" WHERE "messageBoxId" IN ?`;
    let insertQuery = `INSERT INTO "MessageBoxMessagesDeleted" ("messageBoxId", "createdTimestamp", value) VALUES (?, ? ,?)`;
    let counter = 0;

    let result = await sourceClient.execute(
        query,
        [_.union(store.discussionsFromThisTenancyAlone, store.allContentIds)],
        clientOptions
    );

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            let row = rows[i];
            counter++;

            await targetClient.execute(
                insertQuery,
                [row.messageBoxId, row.createdTimestamp, row.value],
                clientOptions
            );
        }
    }

    logger.info(
        `${chalk.green(`✓`)}  Fetched ${
            result.rows.length
        } MessageBoxMessagesDeleted rows...`
    );

    if (_.isEmpty(result.rows)) {
        return;
    }
    await insertAll(targetClient, result.rows);
    logger.info(
        `${chalk.green(
            `✓`
        )}  Inserted ${counter} MessageBoxMessagesDeleted rows...`
    );

    let queryResultOnSource = result;
    result = await target.execute(
        query,
        [_.union(store.discussionsFromThisTenancyAlone, store.allContentIds)],
        clientOptions
    );
    util.compareBothTenants(
        queryResultOnSource.rows.length,
        result.rows.length
    );
};

const copyMessageBoxRecentContributions = async function(
    sourceClient,
    targetClient
) {
    if (_.isEmpty(store.discussionsFromThisTenancyAlone)) {
        logger.info(
            chalk.cyan(
                `✗  Skipped fetching MessageBoxRecentContributions rows...\n`
            )
        );
        return [];
    }
    // MessageBoxRecentContributions
    let query = `SELECT * FROM "MessageBoxRecentContributions" WHERE "messageBoxId" IN ?`;
    let insertQuery = `INSERT INTO "MessageBoxRecentContributions" ("messageBoxId", "contributorId", value) VALUES (?, ?, ?)`;
    let counter = 0;

    let result = await sourceClient.execute(
        query,
        [_.union(store.allContentIds, store.discussionsFromThisTenancyAlone)],
        clientOptions
    );

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            let row = rows[i];
            counter++;

            await targetClient.execute(
                insertQuery,
                [row.messageBoxId, row.contributorId, row.value],
                clientOptions
            );
        }
    }

    logger.info(
        `${chalk.green(`✓`)}  Fetched ${
            result.rows.length
        } MessageBoxRecentContributions rows...`
    );

    if (_.isEmpty(result.rows)) {
        return;
    }
    await insertAll(targetClient, result.rows);
    logger.info(
        `${chalk.green(
            `✓`
        )}  Inserted ${counter} MessageBoxRecentContributions rows...`
    );

    let queryResultOnSource = result;
    result = await targetClient.execute(
        query,
        [_.union(store.allContentIds, store.discussionsFromThisTenancyAlone)],
        clientOptions
    );
    util.compareBothTenants(
        queryResultOnSource.rows.length,
        result.rows.length
    );
};

module.exports = {
    copyMessages,
    copyDiscussions,
    copyMessageBoxMessages,
    copyMessageBoxMessagesDeleted,
    copyMessageBoxRecentContributions
};
