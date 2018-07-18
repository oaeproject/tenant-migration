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
// [ ] winston logs
// [ ] Organize with more files

const _ = require("underscore");
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

const createNewClient = function(dbParams, keyspace) {
    console.log("\nCreating new client for keyspace " + keyspace);
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
    console.log("\nInitializing connection for " + dbParams.keyspace);
    let client = createNewClient(dbParams);

    return client
        .connect()
        .then(() => {
            return keyspaceExists(dbParams, client);
        })
        .then(exists => {
            console.log(
                "\n  keyspace " + dbParams.keyspace + " exists? " + exists
            );
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
            // logs
            console.dir(e, { colors: true });
        });
};

const keyspaceExists = function(dbParams, client) {
    console.log(
        "\nChecking whether keyspace " + dbParams.keyspace + " exists..."
    );
    const query = `SELECT keyspace_name FROM system.schema_keyspaces WHERE keyspace_name = '${
        dbParams.keyspace
    }'`;

    return client
        .execute(query)
        .then(result => {
            return !_.isEmpty(result.rows);
        })
        .catch(e => {
            // logs
            console.dir(e, { colors: true });
        });
};

const createKeyspace = function(dbParams, client) {
    console.log("\nCreating keyspace " + dbParams.keyspace);
    // let client = createNewClient(dbParams);

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
            console.log(
                `√ Created keyspace ${dbParams.keyspace} on target server ${
                    dbParams.host
                }`
            );
            return true;
        })
        .catch(e => {
            // logs
            console.dir(e, { colors: true });
        });
};

let data = {};
let tenantPrincipals = [];
let tenantUsers = [];
let tenantGroups = [];

return (
    initConnection(sourceDatabase)
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

            allPromises = [];
            createAllTables.forEach(eachCreateStatement => {
                allPromises.push(
                    Promise.resolve(
                        data.targetClient.execute(eachCreateStatement.query)
                    )
                );
            });
            return Promise.all(allPromises);
        })
        .then(() => {
            // select everything that describes the tenant
            // We're copying over tables: Tenant, TenantNetwork and TenantNetworkTenants
            let query = `select * from "Tenant" where "alias" = ?`;
            return data.sourceClient.execute(query, [
                sourceDatabase.tenantAlias
            ]);
        })
        .then(result => {
            if (_.isEmpty(result.rows)) {
                // log here
                return;
            }

            let row = result.first();
            let insertQuery = `INSERT into "Tenant" ("alias", "active", "countryCode", "displayName", "emailDomains", "host") VALUES (?, ?, ?, ?, ?, ?)`;

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
                // log here
                return;
            }

            let row = result.first();
            let insertQuery = `INSERT INTO "Config" ("tenantAlias", "configKey", value) VALUES (?, ?, ?)`;
            return data.targetClient.execute(insertQuery, [
                row.tenantAlias,
                row.configKey,
                row.configKey
            ]);
        })
        .then(() => {
            let query = `SELECT * FROM "Principals" WHERE "tenantAlias" = ?`;
            return data.sourceClient.execute(query, [
                sourceDatabase.tenantAlias
            ]);
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
                // log here
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
            return data.targetClient.batch(allInserts, { prepare: true });
        })
        .then(() => {
            // now we copy "PrincipalsByEmail"
            let query = `SELECT * FROM "PrincipalsByEmail" WHERE "principalId" IN ? ALLOW FILTERING`;
            return data.sourceClient.execute(query, [tenantPrincipals]);
        })
        .then(result => {
            if (_.isEmpty(result.rows)) {
                // log here
                return;
            }

            let allInserts = [];
            result.rows.forEach(row => {
                allInserts.push({
                    query: `INSERT INTO "PrincipalsByEmail" (email, "principalId") VALUES (?, ?)`,
                    params: [row.email, row.principalId]
                });
            });
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
                // log here
                return;
            }

            let allInserts = [];
            result.rows.forEach(row => {
                allInserts.push({
                    query: `INSERT INTO "AuthzMembers" ("resourceId", "memberId", role) VALUES (?, ?, ?)`,
                    params: [row.resourceId, row.memberId, row.role]
                });
            });
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

            console.log("Here we go...");
            return doAllTheThings().on("readable", function() {
                // 'readable' is emitted as soon a row is received and parsed
                let row;
                while ((row = this.read())) {
                    // debug
                    console.log(" √ Fetched a folder");

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
                        row.createBy,
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
            // debug
            console.dir(data.folderGroups);
            return data.targetClient.batch(allInserts, { prepare: true });
        })
        .then(() => {
            // query "foldersGroupId"
            let query = `SELECT * FROM "FoldersGroupId" WHERE "groupId" IN ?`;
            return data.sourceClient.execute(query, [data.folderGroups]);
        })
        .then(result => {
            // debug
            console.log("tenant groups:");
            console.dir(tenantGroups);
            console.log("Groups:");
            console.dir(result.rows);

            // insert data into "FoldersGroupId"
            if (_.isEmpty(result.rows)) {
                // log here
                return;
            }

            let allInserts = [];
            result.rows.forEach(row => {
                allInserts.push({
                    query: `INSERT INTO "FoldersGroupId" ("groupId", "folderId") VALUES (?, ?)`,
                    params: [row.groupId, row.folderId]
                });
            });
            return data.targetClient.batch(allInserts, { prepare: true });
        })
        // .then(() => {
        // })
        // .then(() => {
        // })
        // .then(() => {
        // })
        // .then(() => {
        // })
        // .then(() => {
        // query "AuthzMembers"
        // })
        // .then(() => {
        // insert "AuthzMembers"
        // })
        .then(result => {
            process.exit(0);
        })
        .catch(e => {
            console.dir(e);
            process.exit(-1);
        })
);
