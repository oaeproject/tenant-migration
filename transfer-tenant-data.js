/*!
 * Copyright 2014 Apereo Foundation (AF) Licensed under the
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

const _ = require("underscore");
const chalk = require("chalk");
const fs = require("fs");

const logger = require("./logger");
const { initConnection } = require("./db");
let createSchemaQueries = require("./schema.js");

// read json file with source database and keyspace
let fileContents = fs.readFileSync("source-database.json");
const { sourceDatabase } = JSON.parse(fileContents);

// read json file with target database and keyspace
fileContents = fs.readFileSync("target-database.json");
const { targetDatabase } = JSON.parse(fileContents);

let data = {};
let tenantPrincipals = [];
let tenantUsers = [];
let tenantGroups = [];

const createAllTables = async function(targetClient, createAllTables) {
    allPromises = [];
    createAllTables.forEach(eachCreateStatement => {
        allPromises.push(
            Promise.resolve(targetClient.execute(eachCreateStatement.query))
        );
    });
    logger.info(`${chalk.green(`✓`)}  Creating tables...`);
    await Promise.all(allPromises);
};

const selectAllTenants = function(sourceClient) {
    let query = `select * from "Tenant" where "alias" = ?`;
    return sourceClient.execute(query, [sourceDatabase.tenantAlias]);
};

const insertAllTenants = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(`${chalk.green(`✓`)}  No Tenant rows found...`);
        return;
    }

    let row = result.first();
    let insertQuery = `INSERT into "Tenant" ("alias", "active", "countryCode", "displayName", "emailDomains", "host") VALUES (?, ?, ?, ?, ?, ?)`;

    logger.info(`${chalk.green(`✓`)}  Inserting tenant...`);
    await targetClient.execute(insertQuery, [
        row.alias,
        row.active,
        row.countryCode,
        row.displayName,
        row.emailDomains,
        row.host
    ]);
};

const selectTenantConfig = function(sourceClient) {
    let query = `SELECT * FROM "Config" WHERE "tenantAlias" = '${
        sourceDatabase.tenantAlias
    }'`;
    return sourceClient.execute(query);
};

const insertTenantConfig = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(`${chalk.green(`✓`)}  No Config rows found...`);
        return;
    }

    let row = result.first();
    logger.info(`${chalk.green(`✓`)}  Inserting tenant config...`);
    let insertQuery = `INSERT INTO "Config" ("tenantAlias", "configKey", value) VALUES (?, ?, ?)`;
    return targetClient.execute(insertQuery, [
        row.tenantAlias,
        row.configKey,
        row.configKey
    ]);
};

const selectAllPrincipals = function(sourceClient) {
    let query = `SELECT * FROM "Principals" WHERE "tenantAlias" = ?`;
    return sourceClient.execute(query, [sourceDatabase.tenantAlias]);
};

const insertAllPrincipals = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(`${chalk.green(`✓`)}  No Principals rows found...`);
        return;
    }

    // we'll need to know which principals are users or groups
    result.rows.forEach(row => {
        tenantPrincipals.push(row.principalId);
        if (row.principalId.startsWith("g")) {
            tenantGroups.push(row.principalId);
        } else if (row.principalId.startsWith("u")) {
            tenantUsers.push(row.principalId);
        }
    });

    let allInserts = [];
    result.rows.forEach(row => {
        let insertQuery = `INSERT INTO "Principals" ("principalId", "acceptedTC", "admin:global", "admin:tenant", created, "createdBy", deleted, description, "displayName", email, "emailPreference", joinable, "largePictureUri", "lastModified", locale, "mediumPictureUri", "notificationsLastRead", "notificationsUnread", "publicAlias", "smallPictureUri", "tenantAlias", visibility) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
        allInserts.push({
            query: insertQuery,
            params: [
                row.principalId,
                row.acceptedTC,
                row.get("admin:global"),
                row.get("admin:tenant"),
                row.created,
                row.createdBy,
                row.deleted,
                row.description,
                row.displayName,
                row.email,
                row.emailPreference,
                row.joinable,
                row.largePictureUri,
                row.lastModified,
                row.locale,
                row.mediumPictureUri,
                row.notificationsLastRead,
                row.notificationsUnread,
                row.publicAlias,
                row.smallPictureUri,
                row.tenantAlias,
                row.visibility
            ]
        });
    });
    logger.info(`${chalk.green(`✓`)}  Inserting Principals...`);
    await targetClient.batch(allInserts, { prepare: true });
};

const selectPrincipalsByEmail = function(sourceClient) {
    let query = `SELECT * FROM "PrincipalsByEmail" WHERE "principalId" IN ? ALLOW FILTERING`;
    return sourceClient.execute(query, [tenantPrincipals]);
};

const insertAllPrincipalsByEmail = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(`${chalk.green(`✓`)}  No PrincipalsByEmail rows found...`);
        return;
    }

    let allInserts = [];
    result.rows.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "PrincipalsByEmail" (email, "principalId") VALUES (?, ?)`,
            params: [row.email, row.principalId]
        });
    });
    logger.info(`${chalk.green(`✓`)}  Inserting PrincipalsByEmail...`);
    await targetClient.batch(allInserts, { prepare: true });
};

const selectAuthzMembers = function(sourceClient) {
    let query = `SELECT * FROM "AuthzMembers" WHERE "memberId" IN ? ALLOW FILTERING`;
    return sourceClient.execute(query, [tenantPrincipals]);
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

let foldersFromThisTenancyAlone = [];
const selectAllFolders = async function(sourceClient) {
    function doAllTheThings() {
        let query = `SELECT * FROM "Folders"`;
        var com = sourceClient.stream(query);
        var p = new Promise(function(resolve, reject) {
            com.on("end", resolve);
            com.on("error", reject);
        });
        p.on = function() {
            com.on.apply(com, arguments);
            return p;
        };
        return p;
    }

    // query "folders" - This is very very inadequate but we can't filter it!
    let allRows = [];

    await doAllTheThings().on("readable", function() {
        // 'readable' is emitted as soon a row is received and parsed
        let row;
        while ((row = this.read())) {
            if (
                row.tenantAlias &&
                row.tenantAlias === sourceDatabase.tenantAlias
            ) {
                allRows.push(row);
                foldersFromThisTenancyAlone.push(row.id);
            }
        }
    });

    return allRows;
};

let folderGroups = [];
const insertAllFolders = async function(targetClient, result) {
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
        folderGroups.push(row.groupId);
    });
    logger.info(`${chalk.green(`✓`)}  Inserting Folders...`);
    await targetClient.batch(allInserts, { prepare: true });
};

const selectFoldersGroupIds = function(sourceClient) {
    let query = `SELECT * FROM "FoldersGroupId" WHERE "groupId" IN ?`;
    return sourceClient.execute(query, [folderGroups]);
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

const selectAuthzRoles = function(sourceClient) {
    let query = `SELECT * FROM "AuthzRoles" WHERE "principalId" IN ?`;
    return sourceClient.execute(query, [tenantPrincipals]);
};

let allResourceIds = [];
const insertAuthzRoles = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(`${chalk.green(`✓`)}  No AuthzRoles rows found...`);

        return;
    }

    allResourceIds = _.pluck(result.rows, "resourceId");
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

const selectAllContent = function(sourceClient) {
    let query = `SELECT * FROM "Content" WHERE "contentId" IN ?`;
    return sourceClient.execute(query, [allResourceIds]);
};

allContentIds = [];
const insertContent = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(`${chalk.green(`✓`)}  No Content rows found...`);

        return;
    }

    allContentIds = _.pluck(result.rows, "contentId");
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
    return sourceClient.execute(query, [allContentIds]);
};

allRevisionIds = [];
const insertRevisionByContent = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(`${chalk.green(`✓`)}  No RevisionByContent rows found...`);

        return;
    }

    allRevisionIds = _.pluck(result.rows, "revisionId");
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
    return sourceClient.execute(query, [allRevisionIds]);
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

let discussionsFromThisTenancyAlone = [];
const selectDiscussions = async function(sourceClient) {
    // lets query discussions and all messages
    function doAllTheThings() {
        let query = `SELECT * FROM "Discussions"`;
        var com = sourceClient.stream(query);
        var p = new Promise(function(resolve, reject) {
            com.on("end", resolve);
            com.on("error", reject);
        });
        p.on = function() {
            com.on.apply(com, arguments);
            return p;
        };
        return p;
    }

    // query "Discussions" - This is very very inadequate but we can't filter it!
    allRows = [];
    discussionsFromThisTenancyAlone = [];

    await doAllTheThings().on("readable", function() {
        // 'readable' is emitted as soon a row is received and parsed
        let row;
        while ((row = this.read())) {
            if (
                row.tenantAlias &&
                row.tenantAlias === sourceDatabase.tenantAlias
            ) {
                allRows.push(row);
                discussionsFromThisTenancyAlone.push(row.id);
            }
        }
    });
    return allRows;
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

let threadKeysFromThisTenancyAlone = [];
const selectMessageBoxMessages = async function(sourceClient) {
    if (_.isEmpty(discussionsFromThisTenancyAlone)) {
        return [];
    }
    // lets query discussions and all messages
    function doAllTheThings() {
        let query = `SELECT * FROM "MessageBoxMessages" WHERE "messageBoxId" IN ?`;
        var com = sourceClient.stream(query, [discussionsFromThisTenancyAlone]);
        var p = new Promise(function(resolve, reject) {
            com.on("end", resolve);
            com.on("error", reject);
        });
        p.on = function() {
            com.on.apply(com, arguments);
            return p;
        };
        return p;
    }

    // query "Messages" - This is very very inadequate but we can't filter it!
    allRows = [];
    threadKeysFromThisTenancyAlone = [];

    await doAllTheThings().on("readable", function() {
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
    return allRows;
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
    let result = threadKeysFromThisTenancyAlone; // this is an object, not an array
    if (_.isEmpty(result)) {
        return [];
    }
    let messageIds = _.map(threadKeysFromThisTenancyAlone, eachElement => {
        return `${eachElement.messageBoxId}#${eachElement.threadKey.slice(
            0,
            eachElement.threadKey.length - 1
        )}`;
    });
    let query = `SELECT * FROM "Messages" WHERE id IN ?`;
    return sourceClient.execute(query, [messageIds]);
};

const insertMessages = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(`${chalk.green(`✓`)}  No Messages rows found...`);

        return;
    }

    let allInserts = [];
    result.rows.forEach(row => {
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
    if (_.isEmpty(discussionsFromThisTenancyAlone)) {
        return [];
    }
    // MessageBoxMessagesDeleted
    let query = `SELECT * FROM "MessageBoxMessagesDeleted" WHERE "messageBoxId" IN ?`;
    return sourceClient.execute(query, [discussionsFromThisTenancyAlone]);
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
    if (_.isEmpty(discussionsFromThisTenancyAlone)) {
        return [];
    }
    // MessageBoxRecentContributions
    let query = `SELECT * FROM "MessageBoxRecentContributions" WHERE "messageBoxId" IN ?`;
    return sourceClient.execute(query, [discussionsFromThisTenancyAlone]);
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

const selectUsersGroupVisits = function(sourceClient) {
    let query = `SELECT * FROM "UsersGroupVisits" WHERE "userId" IN ?`;
    return sourceClient.execute(query, [tenantPrincipals]);
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

const selectFollowingUsersFollowers = function(sourceClient) {
    let query = `SELECT * FROM "FollowingUsersFollowers" WHERE "userId" IN ?`;
    return sourceClient.execute(query, [tenantPrincipals]);
};

const insertFollowingUsersFollowers = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(
            `${chalk.green(`✓`)}  No FollowingUsersFollowers rows found...`
        );
        return;
    }

    let allInserts = [];
    result.rows.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "FollowingUsersFollowers" ("userId", "followerId", "value") VALUES (?, ?, ?)`,
            params: [row.userId, row.followerId, row.value]
        });
    });
    logger.info(`${chalk.green(`✓`)}  Inserting FollowingUsersFollowers...`);
    await targetClient.batch(allInserts, { prepare: true });
};

const selectFollowingUsersFollowing = function(sourceClient) {
    let query = `SELECT * FROM "FollowingUsersFollowing" WHERE "userId" IN ?`;
    return sourceClient.execute(query, [tenantPrincipals]);
};

const insertFollowingUsersFollowing = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(
            `${chalk.green(`✓`)}  No FollowingUsersFollowing rows found...`
        );
        return;
    }

    let allInserts = [];
    result.rows.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "FollowingUsersFollowing" ("userId", "followingId", "value") VALUES (?, ?, ?)`,
            params: [row.userId, row.followingId, row.value]
        });
    });
    logger.info(`${chalk.green(`✓`)}  Inserting FollowingUsersFollowing...`);
    await targetClient.batch(allInserts, { prepare: true });
};

const selectAuthenticationUserLoginId = function(sourceClient) {
    let query = `SELECT * FROM "AuthenticationUserLoginId" WHERE "userId" IN ?`;
    return sourceClient.execute(query, [tenantPrincipals]);
};

let allLoginIds = [];
const insertAuthenticationUserLoginId = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(
            `${chalk.green(`✓`)}  No AuthenticationUserLoginId rows found...`
        );
        return;
    }

    allLoginIds = _.pluck(result.rows, "loginId");
    let allInserts = [];
    result.rows.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "AuthenticationUserLoginId" ("userId", "loginId", "value") VALUES (?, ?, ?)`,
            params: [row.userId, row.loginId, row.value]
        });
    });
    logger.info(`${chalk.green(`✓`)}  Inserting AuthenticationUserLoginId...`);
    await targetClient.batch(allInserts, { prepare: true });
};

const selectAuthenticationLoginId = function(sourceClient) {
    if (_.isEmpty(allLoginIds)) {
        return [];
    }
    let query = `SELECT * FROM "AuthenticationLoginId" WHERE "loginId" IN ?`;
    return sourceClient.execute(query, [allLoginIds]);
};

const insertAuthenticationUserId = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(
            `${chalk.green(`✓`)}  No AuthenticationLoginId rows found...`
        );
        return;
    }

    let allInserts = [];
    result.rows.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "AuthenticationLoginId" ("loginId", password, secret, "userId") VALUES (?, ?, ?, ?)`,
            params: [row.loginId, row.password, row.secret, row.userId]
        });
    });
    logger.info(`${chalk.green(`✓`)}  Inserting AuthenticationUserLoginId...`);
    await targetClient.batch(allInserts, { prepare: true });
};

const selectOAuthClients = function(sourceClient) {
    let query = `SELECT * FROM "OAuthClient" WHERE id IN ?`;
    if (_.isEmpty(allOauthClientsIds)) {
        return [];
    }

    return sourceClient.execute(query, [allOauthClientsIds]);
};

const insertOAuthClients = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(`${chalk.green(`✓`)}  No OAuthClient rows found...`);
        return;
    }

    let allInserts = [];
    result.rows.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "OAuthClient" (id, "displayName", secret, "userId") VALUES (?, ?, ?, ?)`,
            params: [row.id, row.displayName, row.secret, row.userId]
        });
    });
    logger.info(`${chalk.green(`✓`)}  Inserting OAuthClient...`);
    await targetClient.batch(allInserts, { prepare: true });
};

const selectAuthzInvitations = function(sourceClient) {
    let query = `SELECT * FROM "AuthzInvitations" WHERE "resourceId" IN ?`;
    return sourceClient.execute(query, [allResourceIds]);
};

let allInvitationEmails = [];
const insertAuthzInvitations = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(`${chalk.green(`✓`)}  No AuthzInvitations rows found...`);
        return;
    }

    allInvitationEmails = _.pluck(result.rows, "email");
    let allInserts = [];
    result.rows.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "AuthzInvitations" ("resourceId", email, "inviterUserId", role) VALUES (?, ?, ?, ?)`,
            params: [row.resourceId, row.email, row.inviterUserId, row.role]
        });
    });
    logger.info(`${chalk.green(`✓`)}  Inserting AuthzInvitations...`);
    await targetClient.batch(allInserts, { prepare: true });
};

const selectAuthzInvitationsResourceIdByEmail = function(sourceClient) {
    if (_.isEmpty(allInvitationEmails)) {
        return [];
    }
    let query = `SELECT * FROM "AuthzInvitationsResourceIdByEmail" WHERE email IN ?`;
    return sourceClient.execute(query, [allInvitationEmails]);
};

const insertAuthzInvitationsResourceIdByEmail = async function(
    targetClient,
    result
) {
    if (_.isEmpty(result.rows)) {
        logger.info(
            `${chalk.green(
                `✓`
            )}  No AuthzInvitationsResourceIdByEmail rows found...`
        );
        return;
    }

    let allInserts = [];
    result.rows.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "AuthzInvitationsResourceIdByEmail" (email, "resourceId") VALUES (?, ?)`,
            params: [row.email, row.resourceId]
        });
    });
    logger.info(
        `${chalk.green(`✓`)}  Inserting AuthzInvitationsResourceIdByEmail...`
    );
    await targetClient.batch(allInserts, { prepare: true });
};

const selectAuthzInvitationsTokenByEmail = function(sourceClient) {
    if (_.isEmpty(data.allInvitationEmails)) {
        return [];
    }
    let query = `SELECT * FROM "AuthzInvitationsTokenByEmail" WHERE email IN ?`;
    return sourceClient.execute(query, [data.allInvitationEmails]);
};

const insertAuthzInvitationsTokenByEmail = async function(
    targetClient,
    result
) {
    allInvitationTokens = [];
    if (_.isEmpty(result.rows)) {
        logger.info(
            `${chalk.green(`✓`)}  No AuthzInvitationsTokenByEmail rows found...`
        );
        return;
    }

    data.allInvitationTokens = _.pluck(result.rows, "token");
    let allInserts = [];
    result.rows.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "AuthzInvitationsTokenByEmail" (email, "token") VALUES (?, ?)`,
            params: [row.email, row.token]
        });
    });
    logger.info(
        `${chalk.green(`✓`)}  Inserting AuthzInvitationsTokenIdByEmail...`
    );
    await targetClient.batch(allInserts, { prepare: true });
};
const selectOAuthClientsByUser = function(sourceClient) {
    let query = `SELECT * FROM "OAuthClientsByUser" WHERE "userId" IN ?`;
    return sourceClient.execute(query, [tenantUsers]);
};

let allOauthClientsIds = [];
const insertOAuthClientsByUser = async function(targetClient, result) {
    if (_.isEmpty(result.rows)) {
        logger.info(`${chalk.green(`✓`)}  No OAuthClientsByUser rows found...`);
        return;
    }

    allOauthClientsIds = _.pluck(result.rows, "clientId");
    let allInserts = [];
    result.rows.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "OAuthClientsByUser" ("userId", "clientId", value) VALUES (?, ?, ?)`,
            params: [row.userId, row.clientId, row.value]
        });
    });
    logger.info(`${chalk.green(`✓`)}  Inserting OAuthClientsByUser...`);
    await targetClient.batch(allInserts, { prepare: true });
};

const selectAuthzInvitationsEmailByToken = function(sourceClient) {
    if (_.isEmpty(data.allInvitationTokens)) {
        return [];
    }
    let query = `SELECT * FROM "AuthzInvitationsEmailByToken" WHERE "token" IN ?`;
    return sourceClient.execute(query, [data.allInvitationTokens]);
};

const insertAuthzInvitationsEmailByToken = async function(
    targetClient,
    result
) {
    if (_.isEmpty(result.rows)) {
        logger.info(
            `${chalk.green(`✓`)}  No AuthzInvitationsEmailByToken rows found...`
        );
        return;
    }

    let allInserts = [];
    result.rows.forEach(row => {
        allInserts.push({
            query: `INSERT INTO "AuthzInvitationsEmailByToken" ("token", email) VALUES (?, ?)`,
            params: [row.token, row.email]
        });
    });
    logger.info(
        `${chalk.green(`✓`)}  Inserting AuthzInvitationsEmailByToken...`
    );
    await targetClient.batch(allInserts, { prepare: true });
};

const init = async function() {
    let sourceClient = await initConnection(sourceDatabase);
    let targetClient = await initConnection(targetDatabase);

    try {
        await createAllTables(targetClient, createSchemaQueries);
        await insertAllTenants(
            targetClient,
            await selectAllTenants(sourceClient)
        );
        await insertTenantConfig(
            targetClient,
            await selectTenantConfig(sourceClient)
        );
        await insertAllPrincipals(
            targetClient,
            await selectAllPrincipals(sourceClient)
        );
        await insertAllPrincipalsByEmail(
            targetClient,
            await selectPrincipalsByEmail(sourceClient)
        );
        await insertAllAuthzMembers(
            targetClient,
            await selectAuthzMembers(sourceClient)
        );
        await insertAllFolders(
            targetClient,
            await selectAllFolders(sourceClient)
        );
        await insertFoldersGroupIds(
            targetClient,
            await selectFoldersGroupIds(sourceClient)
        );
        await insertAuthzRoles(
            targetClient,
            await selectAuthzRoles(sourceClient)
        );
        await insertContent(targetClient, await selectAllContent(sourceClient));
        await insertRevisionByContent(
            targetClient,
            await selectRevisionByContent(sourceClient)
        );
        await insertRevisions(
            targetClient,
            await selectRevisions(sourceClient)
        );
        await insertDiscussions(
            targetClient,
            await selectDiscussions(sourceClient)
        );
        await insertMessageBoxMessages(
            targetClient,
            await selectMessageBoxMessages(sourceClient)
        );
        await insertMessages(targetClient, await selectMessages(sourceClient));
        await insertMessageBoxMessagesDeleted(
            targetClient,
            await selectMessageBoxMessagesDeleted(sourceClient)
        );
        await insertMessageBoxRecentContributions(
            targetClient,
            await selectMessageBoxRecentContributions(sourceClient)
        );
        await insertUsersGroupVisits(
            targetClient,
            await selectUsersGroupVisits(sourceClient)
        );
        await insertFollowingUsersFollowers(
            targetClient,
            await selectFollowingUsersFollowers(sourceClient)
        );
        await insertFollowingUsersFollowing(
            targetClient,
            await selectFollowingUsersFollowing(sourceClient)
        );
        await insertAuthenticationUserLoginId(
            targetClient,
            await selectAuthenticationUserLoginId(sourceClient)
        );
        await insertAuthenticationUserId(
            targetClient,
            await selectAuthenticationLoginId(sourceClient)
        );
        await insertOAuthClientsByUser(
            targetClient,
            await selectOAuthClientsByUser(sourceClient)
        );
        await insertOAuthClients(
            targetClient,
            await selectOAuthClients(sourceClient)
        );
        await insertAuthzInvitations(
            targetClient,
            await selectAuthzInvitations(sourceClient)
        );
        await insertAuthzInvitationsResourceIdByEmail(
            targetClient,
            await selectAuthzInvitationsResourceIdByEmail(sourceClient)
        );
        await insertAuthzInvitationsTokenByEmail(
            targetClient,
            await selectAuthzInvitationsTokenByEmail(sourceClient)
        );
        await insertAuthzInvitationsEmailByToken(
            targetClient,
            await selectAuthzInvitationsEmailByToken(sourceClient)
        );
        logger.info(`${chalk.green(`✓`)}  Exiting.`);
    } catch (error) {
        logger.error(`${chalk.red(`✗`)}  Something went wrong: ` + e);
        process.exit(-1);
    } finally {
        logger.end();
        process.exit(0);
    }
};

init();
