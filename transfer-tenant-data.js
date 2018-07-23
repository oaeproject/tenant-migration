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

initConnection(sourceDatabase)
    .then(sourceClient => {
        data.sourceClient = sourceClient;
        return initConnection(targetDatabase);
    })
    .then(targetClient => {
        data.targetClient = targetClient;

        // create all the tables we need
        let createAllTables = require("./schema.js");

        allPromises = [];
        createAllTables.forEach(eachCreateStatement => {
            allPromises.push(
                Promise.resolve(
                    data.targetClient.execute(eachCreateStatement.query)
                )
            );
        });
        logger.info(`${chalk.green(`✓`)}  Creating tables...`);
        return Promise.all(allPromises);
    })
    .then(() => {
        // select everything that describes the tenant
        // We're copying over tables: Tenant, TenantNetwork and TenantNetworkTenants
        let query = `select * from "Tenant" where "alias" = ?`;
        return data.sourceClient.execute(query, [sourceDatabase.tenantAlias]);
    })
    .then(result => {
        if (_.isEmpty(result.rows)) {
            logger.info(`${chalk.green(`✓`)}  No Tenant rows found...`);
            return;
        }

        let row = result.first();
        let insertQuery = `INSERT into "Tenant" ("alias", "active", "countryCode", "displayName", "emailDomains", "host") VALUES (?, ?, ?, ?, ?, ?)`;

        logger.info(`${chalk.green(`✓`)}  Inserting tenant...`);
        return data.targetClient.execute(insertQuery, [
            row.alias,
            row.active,
            row.countryCode,
            row.displayName,
            row.emailDomains,
            row.host
        ]);
    })
    .then(() => {
        // next we copy the "Config" table
        let query = `SELECT * FROM "Config" WHERE "tenantAlias" = '${
            sourceDatabase.tenantAlias
        }'`;
        return data.sourceClient.execute(query);
    })
    .then(result => {
        if (_.isEmpty(result.rows)) {
            logger.info(`${chalk.green(`✓`)}  No Config rows found...`);
            return;
        }

        let row = result.first();
        logger.info(`${chalk.green(`✓`)}  Inserting tenant config...`);
        let insertQuery = `INSERT INTO "Config" ("tenantAlias", "configKey", value) VALUES (?, ?, ?)`;
        return data.targetClient.execute(insertQuery, [
            row.tenantAlias,
            row.configKey,
            row.configKey
        ]);
    })
    .then(() => {
        let query = `SELECT * FROM "Principals" WHERE "tenantAlias" = ?`;
        return data.sourceClient.execute(query, [sourceDatabase.tenantAlias]);
    })
    .then(result => {
        if (_.isEmpty(result.rows)) {
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
        return result.rows;
    })
    .then(result => {
        if (_.isEmpty(result)) {
            logger.info(`${chalk.green(`✓`)}  No Principals rows found...`);
            return;
        }

        let allInserts = [];
        result.forEach(row => {
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
        return data.targetClient.batch(allInserts, { prepare: true });
    })
    .then(() => {
        // now we copy "PrincipalsByEmail"
        let query = `SELECT * FROM "PrincipalsByEmail" WHERE "principalId" IN ? ALLOW FILTERING`;
        return data.sourceClient.execute(query, [tenantPrincipals]);
    })
    .then(result => {
        if (_.isEmpty(result.rows)) {
            logger.info(
                `${chalk.green(`✓`)}  No PrincipalsByEmail rows found...`
            );
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
        return data.targetClient.batch(allInserts, { prepare: true });
    })
    .then(() => {
        // query "authzmembers"
        let query = `SELECT * FROM "AuthzMembers" WHERE "memberId" IN ? ALLOW FILTERING`;
        return data.sourceClient.execute(query, [tenantPrincipals]);
    })
    .then(result => {
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
        return data.targetClient.batch(allInserts, { prepare: true });
    })
    .then(() => {
        function doAllTheThings() {
            let query = `SELECT * FROM "Folders"`;
            var com = data.sourceClient.stream(query);
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
        data.allRows = [];
        data.foldersFromThisTenancyAlone = [];

        return doAllTheThings().on("readable", function() {
            // 'readable' is emitted as soon a row is received and parsed
            let row;
            while ((row = this.read())) {
                if (
                    row.tenantAlias &&
                    row.tenantAlias === sourceDatabase.tenantAlias
                ) {
                    data.allRows.push(row);
                    data.foldersFromThisTenancyAlone.push(row.id);
                }
            }
        });
    })
    .then(result => {
        // Going to insert those filtered folders
        data.folderGroups = [];

        result = data.allRows;
        delete data.allRows;

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
            data.folderGroups.push(row.groupId);
        });
        logger.info(`${chalk.green(`✓`)}  Inserting Folders...`);
        return data.targetClient.batch(allInserts, { prepare: true });
    })
    .then(() => {
        // query "foldersGroupId"
        let query = `SELECT * FROM "FoldersGroupId" WHERE "groupId" IN ?`;
        return data.sourceClient.execute(query, [data.folderGroups]);
    })
    .then(result => {
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
        return data.targetClient.batch(allInserts, { prepare: true });
    })
    .then(() => {
        let query = `SELECT * FROM "AuthzRoles" WHERE "principalId" IN ?`;
        return data.sourceClient.execute(query, [tenantPrincipals]);
    })
    .then(result => {
        if (_.isEmpty(result.rows)) {
            logger.info(`${chalk.green(`✓`)}  No AuthzRoles rows found...`);

            return;
        }

        data.allResourceIds = _.pluck(result.rows, "resourceId");
        let allInserts = [];
        result.rows.forEach(row => {
            allInserts.push({
                query: `INSERT INTO "AuthzRoles" ("principalId", "resourceId", role) VALUES (?, ?, ?)`,
                params: [row.principalId, row.resourceId, row.role]
            });
        });
        logger.info(`${chalk.green(`✓`)}  Inserting AuthzRoles...`);
        return data.targetClient.batch(allInserts, { prepare: true });
    })
    .then(() => {
        let query = `SELECT * FROM "Content" WHERE "contentId" IN ?`;
        return data.sourceClient.execute(query, [data.allResourceIds]);
    })
    .then(result => {
        if (_.isEmpty(result.rows)) {
            logger.info(`${chalk.green(`✓`)}  No Content rows found...`);

            return;
        }

        data.allContentIds = _.pluck(result.rows, "contentId");
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
        return data.targetClient.batch(allInserts, { prepare: true });
    })
    .then(() => {
        let query = `SELECT * FROM "RevisionByContent" WHERE "contentId" IN ?`;
        return data.sourceClient.execute(query, [data.allContentIds]);
    })
    .then(result => {
        if (_.isEmpty(result.rows)) {
            logger.info(
                `${chalk.green(`✓`)}  No RevisionByContent rows found...`
            );

            return;
        }

        data.allRevisionIds = _.pluck(result.rows, "revisionId");
        let allInserts = [];
        result.rows.forEach(row => {
            allInserts.push({
                query: `INSERT INTO "RevisionByContent" ("contentId", created, "revisionId") VALUES (?, ?, ?)`,
                params: [row.contentId, row.created, row.revisionId]
            });
        });
        logger.info(`${chalk.green(`✓`)}  Inserting RevisionByContent...`);
        return data.targetClient.batch(allInserts, { prepare: true });
    })
    .then(() => {
        let query = `SELECT * FROM "Revisions" WHERE "revisionId" IN ?`;
        return data.sourceClient.execute(query, [data.allRevisionIds]);
    })
    .then(result => {
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
        return data.targetClient.batch(allInserts, { prepare: true });
    })
    .then(() => {
        // lets query discussions and all messages
        function doAllTheThings() {
            let query = `SELECT * FROM "Discussions"`;
            var com = data.sourceClient.stream(query);
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
        data.allRows = [];
        data.discussionsFromThisTenancyAlone = [];

        return doAllTheThings().on("readable", function() {
            // 'readable' is emitted as soon a row is received and parsed
            let row;
            while ((row = this.read())) {
                if (
                    row.tenantAlias &&
                    row.tenantAlias === sourceDatabase.tenantAlias
                ) {
                    data.allRows.push(row);
                    data.discussionsFromThisTenancyAlone.push(row.id);
                }
            }
        });
    })
    .then(() => {
        let result = data.allRows;
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
        return data.targetClient.batch(allInserts, { prepare: true });
    })
    .then(() => {
        // lets query discussions and all messages
        function doAllTheThings() {
            let query = `SELECT * FROM "MessageBoxMessages" WHERE "messageBoxId" IN ?`;
            var com = data.sourceClient.stream(query, [
                data.discussionsFromThisTenancyAlone
            ]);
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
        data.allRows = [];
        data.threadKeysFromThisTenancyAlone = [];

        return doAllTheThings().on("readable", function() {
            // 'readable' is emitted as soon a row is received and parsed
            let row;
            while ((row = this.read())) {
                if (row.threadKey) {
                    data.allRows.push(row);
                    data.threadKeysFromThisTenancyAlone.push({
                        messageBoxId: row.messageBoxId,
                        threadKey: _.last(row.threadKey.split("#"))
                    });
                }
            }
        });
    })
    .then(() => {
        // insert "MessageBoxMessages"
        let result = data.allRows;
        if (_.isEmpty(result)) {
            logger.info(
                `${chalk.green(`✓`)}  No MessageBoxMessages rows found...`
            );

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
        return data.targetClient.batch(allInserts, { prepare: true });
    })
    .then(() => {
        let result = data.threadKeysFromThisTenancyAlone; // this is an object, not an array
        let messageIds = _.map(
            data.threadKeysFromThisTenancyAlone,
            eachElement => {
                return `${
                    eachElement.messageBoxId
                }#${eachElement.threadKey.slice(
                    0,
                    eachElement.threadKey.length - 1
                )}`;
            }
        );
        let query = `SELECT * FROM "Messages" WHERE id IN ?`;
        return data.sourceClient.execute(query, [messageIds]);
    })
    .then(result => {
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
        return data.targetClient.batch(allInserts, { prepare: true });
    })
    .then(() => {
        // MessageBoxMessagesDeleted
        let query = `SELECT * FROM "MessageBoxMessagesDeleted" WHERE "messageBoxId" IN ?`;
        return data.sourceClient.execute(query, [
            data.discussionsFromThisTenancyAlone
        ]);
    })
    .then(result => {
        if (_.isEmpty(result.rows)) {
            logger.info(
                `${chalk.green(
                    `✓`
                )}  No MessageBoxMessagesDeleted rows found...`
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
        logger.info(
            `${chalk.green(`✓`)}  Inserting MessageBoxMessagesDeleted...`
        );
        return data.targetClient.batch(allInserts, { prepare: true });
    })
    .then(() => {
        // MessageBoxRecentContributions
        let query = `SELECT * FROM "MessageBoxRecentContributions" WHERE "messageBoxId" IN ?`;
        return data.sourceClient.execute(query, [
            data.discussionsFromThisTenancyAlone
        ]);
    })
    .then(result => {
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
        return data.targetClient.batch(allInserts, { prepare: true });
    })
    .then(() => {
        let query = `SELECT * FROM "UsersGroupVisits" WHERE "userId" IN ?`;
        return data.sourceClient.execute(query, [tenantPrincipals]);
    })
    .then(result => {
        if (_.isEmpty(result.rows)) {
            logger.info(
                `${chalk.green(`✓`)}  No UsersGroupVisits rows found...`
            );

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
        return data.targetClient.batch(allInserts, { prepare: true });
    })
    .then(() => {
        let query = `SELECT * FROM "FollowingUsersFollowers" WHERE "userId" IN ?`;
        return data.sourceClient.execute(query, [tenantPrincipals]);
    })
    .then(result => {
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
        logger.info(
            `${chalk.green(`✓`)}  Inserting FollowingUsersFollowers...`
        );
        return data.targetClient.batch(allInserts, { prepare: true });
    })

    .then(() => {
        let query = `SELECT * FROM "FollowingUsersFollowing" WHERE "userId" IN ?`;
        return data.sourceClient.execute(query, [tenantPrincipals]);
    })
    .then(result => {
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
        logger.info(
            `${chalk.green(`✓`)}  Inserting FollowingUsersFollowing...`
        );
        return data.targetClient.batch(allInserts, { prepare: true });
    })
    .then(() => {
        let query = `SELECT * FROM "AuthenticationUserLoginId" WHERE "userId" IN ?`;
        return data.sourceClient.execute(query, [tenantPrincipals]);
    })
    .then(result => {
        if (_.isEmpty(result.rows)) {
            logger.info(
                `${chalk.green(
                    `✓`
                )}  No AuthenticationUserLoginId rows found...`
            );
            return;
        }

        data.allLoginIds = _.pluck(result.rows, "loginId");
        let allInserts = [];
        result.rows.forEach(row => {
            allInserts.push({
                query: `INSERT INTO "AuthenticationUserLoginId" ("userId", "loginId", "value") VALUES (?, ?, ?)`,
                params: [row.userId, row.loginId, row.value]
            });
        });
        logger.info(
            `${chalk.green(`✓`)}  Inserting AuthenticationUserLoginId...`
        );
        return data.targetClient.batch(allInserts, { prepare: true });
    })
    .then(() => {
        let query = `SELECT * FROM "AuthenticationLoginId" WHERE "loginId" IN ?`;
        return data.sourceClient.execute(query, [data.allLoginIds]);
    })
    .then(result => {
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
        logger.info(
            `${chalk.green(`✓`)}  Inserting AuthenticationUserLoginId...`
        );
        return data.targetClient.batch(allInserts, { prepare: true });
    })
    .then(() => {
        let query = `SELECT * FROM "OAuthClientsByUser" WHERE "userId" IN ?`;
        return data.sourceClient.execute(query, [tenantUsers]);
    })
    .then(result => {
        if (_.isEmpty(result.rows)) {
            logger.info(
                `${chalk.green(`✓`)}  No OAuthClientsByUser rows found...`
            );
            return;
        }

        data.allOauthClientsIds = _.pluck(result.rows, "clientId");
        let allInserts = [];
        result.rows.forEach(row => {
            allInserts.push({
                query: `INSERT INTO "OAuthClientsByUser" ("userId", "clientId", value) VALUES (?, ?, ?)`,
                params: [row.userId, row.clientId, row.value]
            });
        });
        logger.info(`${chalk.green(`✓`)}  Inserting OAuthClientsByUser...`);
        return data.targetClient.batch(allInserts, { prepare: true });
    })
    .then(() => {
        let query = `SELECT * FROM "OAuthClient" WHERE id IN ?`;
        if (_.isEmpty(data.allOauthClientsIds)) {
            return [];
        }

        return data.sourceClient.execute(query, [data.allOauthClientsIds]);
    })
    .then(result => {
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
        return data.targetClient.batch(allInserts, { prepare: true });
    })
    .then(() => {
        let query = `SELECT * FROM "AuthzInvitations" WHERE "resourceId" IN ?`;
        return data.sourceClient.execute(query, [data.allResourceIds]);
    })
    .then(result => {
        data.allInvitationEmails = [];
        if (_.isEmpty(result.rows)) {
            logger.info(
                `${chalk.green(`✓`)}  No AuthzInvitations rows found...`
            );
            return;
        }

        data.allInvitationEmails = _.pluck(result.rows, "email");
        let allInserts = [];
        result.rows.forEach(row => {
            allInserts.push({
                query: `INSERT INTO "AuthzInvitations" ("resourceId", email, "inviterUserId", role) VALUES (?, ?, ?, ?)`,
                params: [row.resourceId, row.email, row.inviterUserId, row.role]
            });
        });
        logger.info(`${chalk.green(`✓`)}  Inserting AuthzInvitations...`);
        return data.targetClient.batch(allInserts, { prepare: true });
    })
    .then(() => {
        if (_.isEmpty(data.allInvitationEmails)) {
            return [];
        }
        let query = `SELECT * FROM "AuthzInvitationsResourceIdByEmail" WHERE email IN ?`;
        return data.sourceClient.execute(query, [data.allInvitationEmails]);
    })
    .then(result => {
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
            `${chalk.green(
                `✓`
            )}  Inserting AuthzInvitationsResourceIdByEmail...`
        );
        return data.targetClient.batch(allInserts, { prepare: true });
    })
    .then(() => {
        if (_.isEmpty(data.allInvitationEmails)) {
            return [];
        }
        let query = `SELECT * FROM "AuthzInvitationsTokenByEmail" WHERE email IN ?`;
        return data.sourceClient.execute(query, [data.allInvitationEmails]);
    })
    .then(result => {
        data.allInvitationTokens = [];
        if (_.isEmpty(result.rows)) {
            logger.info(
                `${chalk.green(
                    `✓`
                )}  No AuthzInvitationsTokenByEmail rows found...`
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
        return data.targetClient.batch(allInserts, { prepare: true });
    })
    .then(() => {
        if (_.isEmpty(data.allInvitationTokens)) {
            return [];
        }
        let query = `SELECT * FROM "AuthzInvitationsEmailByToken" WHERE "token" IN ?`;
        return data.sourceClient.execute(query, [data.allInvitationTokens]);
    })
    .then(result => {
        if (_.isEmpty(result.rows)) {
            logger.info(
                `${chalk.green(
                    `✓`
                )}  No AuthzInvitationsEmailByToken rows found...`
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
        return data.targetClient.batch(allInserts, { prepare: true });
    })
    // .then(() => {})
    // .then(result => {})
    .then(() => {
        logger.info(`${chalk.green(`✓`)}  Exiting.`);
        logger.end();
        process.exit(0);
    })
    .catch(e => {
        logger.error(`${chalk.red(`✗`)}  Something went wrong: ` + e);
        logger.end();
        process.exit(-1);
    });
// .finally(() => {
// });
