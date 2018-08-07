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

const clientOptions = {
    fetchSize: 999999,
    prepare: true
};

const copyAllFolders = async function(sourceClient, targetClient) {
    let query = `SELECT * FROM "Folders"`;
    let insertQuery = `INSERT INTO "Folders" (id, created, "createdBy", description, "displayName", "groupId", "lastModified", previews, "tenantAlias", visibility) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ? )`;
    let counter = 0;
    let allRows = [];

    function doAllTheThings() {
        var com = sourceClient.stream(query);
        var p = new Promise(function(resolve, reject) {
            // com.on("end", resolve(allRows))
            com.on("end", async function() {
                logger.info(
                    `${chalk.green(`✓`)}  Fetched ${
                        allRows.length
                    } Folders rows...`
                );
                if (_.isEmpty(allRows)) {
                    return;
                }
                await insertAll(targetClient, allRows);
                logger.info(
                    `${chalk.green(`✓`)}  Inserted ${counter} Folders rows...\n`
                );
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

    await doAllTheThings().on("readable", async function() {
        // 'readable' is emitted as soon a row is received and parsed
        let row;
        while ((row = this.read())) {
            if (
                row.tenantAlias &&
                row.tenantAlias === sourceDatabase.tenantAlias
            ) {
                allRows.push(row);
                store.folderGroupIdsFromThisTenancyAlone.push(row.groupId);
            }
        }
    });
};

const copyFoldersGroupIds = async function(sourceClient, targetClient) {
    if (_.isEmpty(store.folderGroupIdsFromThisTenancyAlone)) {
        logger.info(
            `${chalk.green(`✗`)}  Skipped fetching FoldersGroupId rows...`
        );
        return [];
    }
    let query = `SELECT * FROM "FoldersGroupId" WHERE "groupId" IN ?`;
    let insertQuery = `INSERT INTO "FoldersGroupId" ("groupId", "folderId") VALUES (?, ?)`;
    let counter = 0;

    let result = await sourceClient.execute(query, [
        store.folderGroupIdsFromThisTenancyAlone
    ]);

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            let row = rows[i];
            counter++;

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
    await insertAll(targetClient, result.rows);
    logger.info(
        `${chalk.green(`✓`)}  Inserted ${counter} FoldersGroupId rows...\n`
    );
};

module.exports = {
    copyAllFolders,
    copyFoldersGroupIds
};
