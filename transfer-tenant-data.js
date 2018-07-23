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

// TODO
// [x] promisify some bits
// [x] winston logs
// [ ] Organize with more files

const _ = require("underscore");
const chalk = require("chalk");
const { createLogger, format, transports } = require("winston");
const { combine, timestamp, label, printf } = format;
const cassandra = require("cassandra-driver");
const dataTypes = require("cassandra-driver").types.dataTypes;
const Row = require("cassandra-driver").types.Row;
const Cassandra = require("oae-util/lib/cassandra");

/**
 * Here's how it works
 * 1 We fetch all the tenancy data so that we create it somewhere else
 */

const sourceDatabase = {
    keyspace: "oae",
    host: "localhost",
    timeout: 3000,
    tenantAlias: "uc",
    strategyClass: "SimpleStrategy",
    replication: 1
};

const targetDatabase = {
    keyspace: "oaeTransfer",
    host: "localhost",
    timeout: 3000,
    tenantAlias: "uc",
    strategyClass: "SimpleStrategy",
    replication: 1
};

const myFormat = printf(info => {
    return `${info.timestamp} [${info.label}] ${info.level}: ${info.message}`;
});

const logger = createLogger({
    level: "info",
    format: combine(
        label({ label: "Tenancy data migration" }),
        timestamp(),
        myFormat
    ),
    transports: [new transports.Console()]
});

const createNewClient = function(dbParams, keyspace) {
    const loadBalancingPolicy = new cassandra.policies.loadBalancing.RoundRobinPolicy();
    const reconnectionPolicy = new cassandra.policies.reconnection.ConstantReconnectionPolicy(
        dbParams.timeout
    );

    let config = {
        contactPoints: [dbParams.host],
        policies: {
            timestampGeneration: null,
            loadBalancing: loadBalancingPolicy,
            reconnection: reconnectionPolicy
        },
        // keyspace: keyspace,
        protocolOptions: { maxVersion: 3 },
        socketOptions: {
            connectTimeout: dbParams.timeout
        },
        consistency: cassandra.types.consistencies.quorum
    };

    if (keyspace) {
        config.keyspace = keyspace;
    }

    return new cassandra.Client(config);
};

const initConnection = function(dbParams) {
    logger.info(
        `${chalk.green(`✓`)}  Initialising connection to ${dbParams.host}/${
            dbParams.keyspace
        }`
    );
    let client = createNewClient(dbParams);

    return client
        .connect()
        .then(() => {
            return keyspaceExists(dbParams, client);
        })
        .then(exists => {
            if (!exists) {
                return createKeyspace(dbParams, client);
            } else {
                return;
            }
        })
        .then(() => {
            client = createNewClient(dbParams, dbParams.keyspace);
            return client;
        })
        .catch(e => {
            logger.error(`${chalk.red(`✗`)}  Something went wrong: ` + e);
            process.exit(-1);
        });
};

const keyspaceExists = function(dbParams, client) {
    const query = `SELECT keyspace_name FROM system.schema_keyspaces WHERE keyspace_name = '${
        dbParams.keyspace
    }'`;

    return client
        .execute(query)
        .then(result => {
            return !_.isEmpty(result.rows);
        })
        .catch(e => {
            logger.error(`${chalk.red(`✗`)}  Something went wrong: ` + e);
            process.exit(-1);
        });
};

const createKeyspace = function(dbParams, client) {
    var options = {
        name: dbParams.keyspace,
        strategyClass: dbParams.strategyClass,
        replication: dbParams.replication
    };

    const query = `CREATE KEYSPACE IF NOT EXISTS "${
        dbParams.keyspace
    }" WITH REPLICATION = { 'class': '${
        dbParams.strategyClass
    }', 'replication_factor': ${dbParams.replication} };`;

    return client
        .execute(query)
        .then(result => {
            logger.info(
                `${chalk.green(`✓`)}  Created keyspace ${
                    dbParams.keyspace
                } on ${dbParams.host}`
            );
            return true;
        })
        .catch(e => {
            logger.error(`${chalk.red(`✗`)}  Something went wrong: ` + e);
            process.exit(-1);
        });
};

let data = {};
let tenantPrincipals = [];
let tenantUsers = [];
let tenantGroups = [];

return initConnection(sourceDatabase)
    .then(sourceClient => {
        data.sourceClient = sourceClient;
        return initConnection(targetDatabase);
    })
    .then(targetClient => {
        data.targetClient = targetClient;

        // create all the tables we need
        let createAllTables = [];
        createAllTables.push({
            query: `CREATE TABLE IF NOT EXISTS "Principals" ("principalId" text PRIMARY KEY, "tenantAlias" text, "displayName" text, "description" text, "email" text, "emailPreference" text, "visibility" text, "joinable" text, "lastModified" text, "locale" text, "publicAlias" text, "largePictureUri" text, "mediumPictureUri" text, "smallPictureUri" text, "admin:global" text, "admin:tenant" text, "notificationsUnread" text, "notificationsLastRead" text, "acceptedTC" text, "createdBy" text, "created" timestamp, "deleted" timestamp)`,
            params: []
        });
        createAllTables.push({
            query: `CREATE TABLE IF NOT EXISTS "PrincipalsByEmail" ("email" text, "principalId" text, PRIMARY KEY ("email", "principalId"))`,
            params: []
        });
        createAllTables.push({
            query: `CREATE TABLE IF NOT EXISTS "Tenant" ("alias" text PRIMARY KEY, "displayName" text, "host" text, "emailDomains" text, "countryCode" text, "active" boolean)`,
            params: []
        });
        createAllTables.push({
            query: `CREATE TABLE IF NOT EXISTS "Folders" ("id" text PRIMARY KEY, "tenantAlias" text, "groupId" text, "displayName" text, "visibility" text, "description" text, "createdBy" text, "created" bigint, "lastModified" bigint, "previews" text)`,
            params: []
        });
        createAllTables.push({
            query: `CREATE TABLE IF NOT EXISTS "FoldersGroupId" ("groupId" text PRIMARY KEY, "folderId" text)`,
            params: []
        });
        createAllTables.push({
            query: `CREATE TABLE IF NOT EXISTS "AuthzMembers" ("resourceId" text, "memberId" text, "role" text, PRIMARY KEY ("resourceId", "memberId")) WITH COMPACT STORAGE`,
            params: []
        });
        createAllTables.push({
            query: `CREATE TABLE IF NOT EXISTS "AuthzRoles" ("principalId" text, "resourceId" text, "role" text, PRIMARY KEY ("principalId", "resourceId")) WITH COMPACT STORAGE`,
            params: []
        });
        createAllTables.push({
            query: `CREATE TABLE IF NOT EXISTS "Content" ("contentId" text PRIMARY KEY, "tenantAlias" text, "visibility" text, "displayName" text, "description" text, "resourceSubType" text, "createdBy" text, "created" text, "lastModified" text, "latestRevisionId" text, "uri" text, "previews" text, "status" text, "largeUri" text, "mediumUri" text, "smallUri" text, "thumbnailUri" text, "wideUri" text, "etherpadGroupId" text, "etherpadPadId" text, "filename" text, "link" text, "mime" text, "size" text)`,
            params: []
        });
        createAllTables.push({
            query:
                'CREATE TABLE IF NOT EXISTS "RevisionByContent" ("contentId" text, "created" text, "revisionId" text, PRIMARY KEY ("contentId", "created")) WITH COMPACT STORAGE'
        });
        createAllTables.push({
            query: `CREATE TABLE IF NOT EXISTS "Revisions" ("revisionId" text PRIMARY KEY, "contentId" text, "created" text, "createdBy" text, "filename" text, "mime" text, "size" text, "uri" text, "previewsId" text, "previews" text, "status" text, "largeUri" text, "mediumUri" text, "smallUri" text, "thumbnailUri" text, "wideUri" text, "etherpadHtml" text)`
        });
        createAllTables.push({
            query:
                'CREATE TABLE IF NOT EXISTS "Discussions" ("id" text PRIMARY KEY, "tenantAlias" text, "displayName" text, "visibility" text, "description" text, "createdBy" text, "created" text, "lastModified" text)'
        });
        createAllTables.push({
            query:
                'CREATE TABLE IF NOT EXISTS "Messages" ("id" text PRIMARY KEY, "threadKey" text, "createdBy" text, "body" text, "deleted" text)'
        });
        createAllTables.push({
            query:
                'CREATE TABLE IF NOT EXISTS "MessageBoxMessages" ("messageBoxId" text, "threadKey" text, "value" text, PRIMARY KEY ("messageBoxId", "threadKey")) WITH COMPACT STORAGE'
        });
        createAllTables.push({
            query:
                'CREATE TABLE IF NOT EXISTS "MessageBoxMessagesDeleted" ("messageBoxId" text, "createdTimestamp" text, "value" text, PRIMARY KEY ("messageBoxId", "createdTimestamp")) WITH COMPACT STORAGE'
        });
        createAllTables.push({
            query:
                'CREATE TABLE IF NOT EXISTS "MessageBoxRecentContributions" ("messageBoxId" text, "contributorId" text, "value" text, PRIMARY KEY ("messageBoxId", "contributorId")) WITH COMPACT STORAGE'
        });
        createAllTables.push({
            query:
                'CREATE TABLE IF NOT EXISTS "FollowingUsersFollowers" ("userId" text, "followerId" text, "value" text, PRIMARY KEY ("userId", "followerId")) WITH COMPACT STORAGE'
        });
        createAllTables.push({
            query:
                'CREATE TABLE IF NOT EXISTS "FollowingUsersFollowing" ("userId" text, "followingId" text, "value" text, PRIMARY KEY ("userId", "followingId")) WITH COMPACT STORAGE'
        });
        createAllTables.push({
            query:
                'CREATE TABLE IF NOT EXISTS "UsersGroupVisits" ("userId" text, "groupId" text, "latestVisit" text, PRIMARY KEY ("userId", "groupId"))'
        });

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
    .then(result => {
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
