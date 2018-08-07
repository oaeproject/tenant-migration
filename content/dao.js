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

const clientOptions = {
    fetchSize: 999999,
    prepare: true
};

const copyAllContent = async function(sourceClient, targetClient) {
    let query = `SELECT * FROM "Content" WHERE "contentId" IN ?`;
    let insertQuery = `INSERT INTO "Content" ("contentId", created, "createdBy", description, "displayName", "etherpadGroupId", "etherpadPadId", filename, "largeUri", "lastModified", "latestRevisionId", link, "mediumUri", mime, previews, "resourceSubType", size, "smallUri", status, "tenantAlias", "thumbnailUri", uri, visibility, "wideUri") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
    let counter = 0;

    let result = await sourceClient.execute(
        query,
        [store.allResourceIds],
        clientOptions
    );
    store.allContentIds = _.pluck(result.rows, "contentId");

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            let row = rows[i];
            counter++;

            await targetClient.execute(
                insertQuery,
                [
                    row.contentId,
                    row.created,
                    row.createdBy,
                    row.description,
                    row.displayName,
                    row.etherpadGroupId,
                    row.etherpadPadId,
                    row.filename,
                    row.largeUri,
                    row.lastModified,
                    row.latestRevisionId,
                    row.link,
                    row.mediumUri,
                    row.mime,
                    row.previews,
                    row.resourceSubType,
                    row.size,
                    row.smallUri,
                    row.status,
                    row.tenantAlias,
                    row.thumbnailUri,
                    row.uri,
                    row.visibility,
                    row.wideUri
                ],
                clientOptions
            );
        }
    }

    logger.info(
        `${chalk.green(`✓`)}  Fetched ${result.rows.length} Content rows...`
    );
    if (_.isEmpty(result.rows)) {
        return;
    }
    await insertAll(targetClient, result.rows);
    logger.info(
        `${chalk.green(`✓`)}  Inserted ${counter} AuthzRoles rows...\n`
    );
};

const copyRevisionByContent = async function(sourceClient, targetClient) {
    let query = `SELECT * FROM "RevisionByContent" WHERE "contentId" IN ?`;
    let insertQuery = `INSERT INTO "RevisionByContent" ("contentId", created, "revisionId") VALUES (?, ?, ?)`;
    let counter = 0;

    let result = await sourceClient.execute(
        query,
        [store.allContentIds],
        clientOptions
    );
    store.allRevisionIds = _.pluck(result.rows, "revisionId");

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            let row = rows[i];
            counter++;

            await targetClient.execute(
                insertQuery,
                [row.contentId, row.created, row.revisionId],
                clientOptions
            );
        }
    }

    logger.info(
        `${chalk.green(`✓`)}  Fetched ${
            result.rows.length
        } RevisionByContent rows...`
    );
    if (_.isEmpty(result.rows)) {
        return;
    }
    await insertAll(targetClient, result.rows);
    logger.info(
        `${chalk.green(`✓`)}  Inserted ${counter} RevisionByContent rows...\n`
    );
};

const copyRevisions = async function(sourceClient, targetClient) {
    let query = `SELECT * FROM "Revisions" WHERE "revisionId" IN ?`;
    let insertQuery = `INSERT INTO "Revisions" ("revisionId", "contentId", created, "createdBy", "etherpadHtml", filename, "largeUri", "mediumUri", mime, previews, "previewsId", size, "smallUri", status, "thumbnailUri", uri, "wideUri") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
    let counter = 0;

    let result = await sourceClient.execute(
        query,
        [store.allRevisionIds],
        clientOptions
    );

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            let row = rows[i];
            counter++;

            await targetClient.execute(
                insertQuery,
                [
                    row.revisionId,
                    row.contentId,
                    row.created,
                    row.createdBy,
                    row.etherpadHtml,
                    row.filename,
                    row.largeUri,
                    row.mediumUri,
                    row.mime,
                    row.previews,
                    row.previewsId,
                    row.size,
                    row.smallUri,
                    row.status,
                    row.thumbnailUri,
                    row.uri,
                    row.wideUri
                ],
                clientOptions
            );
        }
    }

    logger.info(
        `${chalk.green(`✓`)}  Fetched ${result.rows.length} Revisions rows...`
    );
    if (_.isEmpty(result.rows)) {
        return;
    }
    await insertAll(targetClient, result.rows);
    logger.info(`${chalk.green(`✓`)}  Inserted ${counter} Revisions rows...\n`);
};

module.exports = {
    copyAllContent,
    copyRevisionByContent,
    copyRevisions
};
