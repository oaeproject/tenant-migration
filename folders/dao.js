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
const util = require("../util");
const { Store } = require("../store");

const clientOptions = {
    fetchSize: 999999,
    prepare: true
};

const copyAllFolders = async function(source, target) {
    const query = `SELECT * FROM "Folders" WHERE "tenantAlias" = ? LIMIT ${
        clientOptions.fetchSize
    }`;
    const insertQuery = `INSERT INTO "Folders" (id, created, "createdBy", description, "displayName", "groupId", "lastModified", previews, "tenantAlias", visibility) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ? )`;

    let result = await source.client.execute(
        query,
        [source.database.tenantAlias],
        clientOptions
    );
    let folderGroupIdsFromThisTenancyAlone = _.pluck(result.rows, "groupId");
    Store.setAttribute(
        "folderGroupIdsFromThisTenancyAlone",
        folderGroupIdsFromThisTenancyAlone
    );

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
                    row.groupId,
                    row.lastModified,
                    row.previews,
                    row.tenantAlias,
                    row.visibility
                ],
                clientOptions
            );
        }
    }

    logger.info(
        `${chalk.green(`✓`)}  Fetched ${result.rows.length} Folders rows...`
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

const copyFoldersGroupIds = async function(source, target) {
    if (_.isEmpty(Store.getAttribute("folderGroupIdsFromThisTenancyAlone"))) {
        logger.info(chalk.cyan(`✗  Skipped fetching FoldersGroupId rows...\n`));
        return [];
    }
    const query = `SELECT * FROM "FoldersGroupId" WHERE "groupId" IN ? LIMIT ${
        clientOptions.fetchSize
    }`;
    const insertQuery = `INSERT INTO "FoldersGroupId" ("groupId", "folderId") VALUES (?, ?)`;

    let result = await source.client.execute(
        query,
        [Store.getAttribute("folderGroupIdsFromThisTenancyAlone")],
        clientOptions
    );

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];

            await targetClient.execute(
                insertQuery,
                [row.groupId, row.folderId],
                clientOptions
            );
        }
    }

    logger.info(
        `${chalk.green(`✓`)}  Fetched ${
            result.rows.length
        } FoldersGroupId rows...`
    );
    if (_.isEmpty(result.rows)) {
        return;
    }
    await insertAll(target.client, result.rows);

    const queryResultOnSource = result;
    result = await target.client.execute(
        query,
        [Store.getAttribute("folderGroupIdsFromThisTenancyAlone")],
        clientOptions
    );
    util.compareResults(queryResultOnSource.rows.length, result.rows.length);
};

module.exports = {
    copyAllFolders,
    copyFoldersGroupIds
};
