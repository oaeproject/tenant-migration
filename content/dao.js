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

const selectAllContent = function(sourceClient) {
    let query = `SELECT * FROM "Content" WHERE "contentId" IN ?`;
    return sourceClient.execute(query, [store.allResourceIds]);
};

const insertContent = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(`${chalk.green(`✓`)}  No Content rows found...`);

        return;
    }

    store.allContentIds = _.pluck(result.rows, "contentId");
    let allInserts = [];
    result.rows.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "Content" ("contentId", created, "createdBy", description, "displayName", "etherpadGroupId", "etherpadPadId", filename, "largeUri", "lastModified", "latestRevisionId", link, "mediumUri", mime, previews, "resourceSubType", size, "smallUri", status, "tenantAlias", "thumbnailUri", uri, visibility, "wideUri") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            params: [
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
            ]
        });
    });
    logger.info(`${chalk.green(`✓`)}  Inserting AuthzRoles...`);
    await targetClient.batch(allInserts, { prepare: true });
};

const selectRevisionByContent = function(sourceClient) {
    let query = `SELECT * FROM "RevisionByContent" WHERE "contentId" IN ?`;
    return sourceClient.execute(query, [store.allContentIds]);
};

const insertRevisionByContent = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(`${chalk.green(`✓`)}  No RevisionByContent rows found...`);

        return;
    }

    store.allRevisionIds = _.pluck(result.rows, "revisionId");
    let allInserts = [];
    result.rows.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "RevisionByContent" ("contentId", created, "revisionId") VALUES (?, ?, ?)`,
            params: [row.contentId, row.created, row.revisionId]
        });
    });
    logger.info(`${chalk.green(`✓`)}  Inserting RevisionByContent...`);
    await targetClient.batch(allInserts, { prepare: true });
};

const selectRevisions = function(sourceClient) {
    let query = `SELECT * FROM "Revisions" WHERE "revisionId" IN ?`;
    return sourceClient.execute(query, [store.allRevisionIds]);
};

const insertRevisions = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(`${chalk.green(`✓`)}  No Revisions rows found...`);

        return;
    }

    let allInserts = [];
    result.rows.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "Revisions" ("revisionId", "contentId", created, "createdBy", "etherpadHtml", filename, "largeUri", "mediumUri", mime, previews, "previewsId", size, "smallUri", status, "thumbnailUri", uri, "wideUri") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            params: [
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
            ]
        });
    });
    logger.info(`${chalk.green(`✓`)}  Inserting Revisions...`);
    await targetClient.batch(allInserts, { prepare: true });
};

module.exports = {
    selectAllContent,
    selectRevisionByContent,
    selectRevisions,
    insertContent,
    insertRevisionByContent,
    insertRevisions
};
