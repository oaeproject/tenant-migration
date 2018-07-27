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

const selectAllFolders = async function(sourceClient) {
    let allRows = [];

    function doAllTheThings() {
        let query = `SELECT * FROM "Folders"`;
        var com = sourceClient.stream(query);
        var p = new Promise(function(resolve, reject) {
            // com.on("end", resolve(allRows));
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

const insertAllFolders = async function(targetClient, result) {
    if (_.isEmpty(result)) {
        logger.info(`${chalk.green(`✓`)}  No Folders rows found...`);

        return;
    }

    let allInserts = [];
    result.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "Folders" (id, created, "createdBy", description, "displayName", "groupId", "lastModified", previews, "tenantAlias", visibility) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ? )`,
            params: [
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
            ]
        });
        // store.folderGroups.push(row.groupId);
    });
    logger.info(`${chalk.green(`✓`)}  Inserting Folders...`);
    await targetClient.batch(allInserts, { prepare: true });
};

const selectFoldersGroupIds = function(sourceClient) {
    if (_.isEmpty(store.folderGroupIdsFromThisTenancyAlone)) {
        return [];
    }
    let query = `SELECT * FROM "FoldersGroupId" WHERE "groupId" IN ?`;
    return sourceClient.execute(query, [
        store.folderGroupIdsFromThisTenancyAlone
    ]);
};

const insertFoldersGroupIds = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(`${chalk.green(`✓`)}  No FoldersGroupId rows found...`);

        return;
    }
    let allInserts = [];
    result.rows.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "FoldersGroupId" ("groupId", "folderId") VALUES (?, ?)`,
            params: [row.groupId, row.folderId]
        });
    });
    logger.info(`${chalk.green(`✓`)}  Inserting FoldersGroupId...`);
    await targetClient.batch(allInserts, { prepare: true });
};

module.exports = {
    insertAllFolders,
    insertFoldersGroupIds,
    selectAllFolders,
    selectFoldersGroupIds
};
