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

const copyDiscussions = async function(source, target) {
    const query = `
      SELECT *
      FROM "Discussions"
      WHERE "tenantAlias" = ?
      LIMIT ${clientOptions.fetchSize}`;
    const insertQuery = `
      INSERT INTO "Discussions" (
          id,
          created,
          "createdBy",
          description,
          "displayName",
          "lastModified",
          "tenantAlias",
          visibility)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)`;

    let result = await source.client.execute(
        query,
        [source.database.tenantAlias],
        clientOptions
    );
    let discussionsFromThisTenancyAlone = _.pluck(result.rows, "id");
    Store.setAttribute(
        "discussionsFromThisTenancyAlone",
        _.uniq(discussionsFromThisTenancyAlone)
    );

    // Query "Discussions" - This is very very inadequate but we can't filter it!
    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];

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

    logger.info(
        `${chalk.green(`✓`)}  Fetched ${result.rows.length} Discussions rows...`
    );
    if (_.isEmpty(result.rows)) {
        return;
    }
    await insertAll(target.client, result.rows);

    const queryResultOnSource = result;
    result = await target.client.execute(
        query,
        [source.database.tenantAlias],
        clientOptions
    );
    util.compareResults(queryResultOnSource.rows.length, result.rows.length);
};

const copyMessageBoxMessages = async function(source, target) {
    const query = `
      SELECT *
      FROM "MessageBoxMessages"
      WHERE "messageBoxId"
      IN ?
      LIMIT ${clientOptions.fetchSize}`;
    const insertQuery = `
      INSERT INTO "MessageBoxMessages" (
          "messageBoxId",
          "threadKey",
          value)
          VALUES (?, ?, ?)`;
    let counter = 0;
    let allRows = [];
    let threadKeysFromThisTenancyAlone = [];

    if (_.isEmpty(Store.getAttribute("allResourceIds"))) {
        logger.info(
            chalk.cyan(`✗  Skipped fetching MessageBoxMessages rows...\n`)
        );
        return [];
    }

    // Lets query discussions and all messages
    function doAllTheThings() {
        const com = source.client.stream(query, [
            _.union(
                Store.getAttribute("allContentIds"),
                Store.getAttribute("discussionsFromThisTenancyAlone"),
                Store.getAttribute("allResourceIds")
            )
        ]);
        const p = new Promise((resolve, reject) => {
            com.on("end", async () => {
                Store.setAttribute(
                    "threadKeysFromThisTenancyAlone",
                    _.uniq(threadKeysFromThisTenancyAlone)
                );
                logger.info(
                    `${chalk.green(`✓`)}  Fetched ${
                        allRows.length
                    } MessageBoxMessages rows...`
                );
                if (_.isEmpty(allRows)) {
                    return;
                }
                await insertAll(target.client, allRows);
                util.compareResults(allRows.length, counter);
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
            const row = rows[i];
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
                threadKeysFromThisTenancyAlone.push({
                    messageBoxId: row.messageBoxId,
                    threadKey: _.last(row.threadKey.split("#"))
                });
            }
        }
    });
};

const copyMessages = async function(source, target) {
    const query = `
      SELECT *
      FROM "Messages"
      LIMIT ${clientOptions.fetchSize}`;
    const insertQuery = `
      INSERT INTO "Messages" (
          id,
          body,
          "createdBy",
          deleted,
          "threadKey")
          VALUES (?, ?, ?, ?, ?)`;
    let counter = 0;
    let allRows = [];
    let allTenantMessages = [];

    if (_.isEmpty(Store.getAttribute("allResourceIds"))) {
        logger.info(chalk.cyan(`✗  Skipped fetching Messages rows...\n`));
        return [];
    }
    // Lets query discussions and all messages
    function doAllTheThings() {
        const com = source.client.stream(query);
        const p = new Promise((resolve, reject) => {
            com.on("end", async () => {
                Store.setAttribute(
                    "allTenantMessages",
                    _.uniq(allTenantMessages)
                );
                logger.info(
                    `${chalk.green(`✓`)}  Fetched ${
                        allRows.length
                    } Messages rows...`
                );

                if (_.isEmpty(allRows)) {
                    return;
                }
                await insertAll(target.client, allRows);
                util.compareResults(allRows.length, counter);
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
            const row = rows[i];
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
            if (
                row.id &&
                row.id.split(":")[1] === source.database.tenantAlias
            ) {
                allRows.push(row);
                allTenantMessages.push(row.id);
            }
        }
    });
};

const copyMessageBoxMessagesDeleted = async function(source, target) {
    // if (_.isEmpty(Store.getAttribute("discussionsFromThisTenancyAlone"))) {
    if (_.isEmpty(Store.getAttribute("allResourceIds"))) {
        logger.info(
            chalk.cyan(
                `✗  Skipped fetching MessageBoxMessagesDeleted rows...\n`
            )
        );
        return [];
    }
    // MessageBoxMessagesDeleted
    const query = `
      SELECT *
      FROM "MessageBoxMessagesDeleted"
      WHERE "messageBoxId"
      IN ?
      LIMIT ${clientOptions.fetchSize}`;
    const insertQuery = `
      INSERT INTO "MessageBoxMessagesDeleted" (
          "messageBoxId",
          "createdTimestamp",
          value)
          VALUES (?, ? ,?)`;

    let discussionsAndContentIds = _.union(
        Store.getAttribute("discussionsFromThisTenancyAlone"),
        Store.getAttribute("allContentIds"),
        Store.getAttribute("allResourceIds")
    );
    let result = await source.client.execute(
        query,
        [discussionsAndContentIds],
        clientOptions
    );

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];

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
    await insertAll(target.client, result.rows);

    const queryResultOnSource = result;
    result = await target.client.execute(
        query,
        [discussionsAndContentIds],
        clientOptions
    );
    util.compareResults(queryResultOnSource.rows.length, result.rows.length);
};

const copyMessageBoxRecentContributions = async function(source, target) {
    if (_.isEmpty(Store.getAttribute("allResourceIds"))) {
        logger.info(
            chalk.cyan(
                `✗  Skipped fetching MessageBoxRecentContributions rows...\n`
            )
        );
        return [];
    }
    // MessageBoxRecentContributions
    const query = `
      SELECT *
      FROM "MessageBoxRecentContributions"
      WHERE "messageBoxId"
      IN ?
      LIMIT ${clientOptions.fetchSize}`;
    const insertQuery = `
      INSERT INTO "MessageBoxRecentContributions" (
          "messageBoxId",
          "contributorId",
          value)
          VALUES (?, ?, ?)`;

    let discussionsAndContentIds = _.union(
        Store.getAttribute("allContentIds"),
        Store.getAttribute("discussionsFromThisTenancyAlone"),
        Store.getAttribute("allResourceIds")
    );

    let result = await source.client.execute(
        query,
        [discussionsAndContentIds],
        clientOptions
    );

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];

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
    await insertAll(target.client, result.rows);

    const queryResultOnSource = result;
    result = await target.client.execute(
        query,
        [discussionsAndContentIds],
        clientOptions
    );
    util.compareResults(queryResultOnSource.rows.length, result.rows.length);
};

module.exports = {
    copyMessages,
    copyDiscussions,
    copyMessageBoxMessages,
    copyMessageBoxMessagesDeleted,
    copyMessageBoxRecentContributions
};
