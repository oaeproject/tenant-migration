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
        keyspace: keyspace,
        protocolOptions: { maxVersion: 3 },
        socketOptions: {
            connectTimeout: dbParams.timeout
        },
        consistency: cassandra.types.consistencies.quorum
    };

    return new cassandra.Client(config);
};

const initConnection = function(dbParams) {
    let client = createNewClient(dbParams, dbParams.keyspace);

    return client
        .connect()
        .then(() => {
            return keyspaceExists(dbParams, client);
        })
        .then(exists => {
            if (!exists) {
                return createKeyspace(dbParams);
            }
            return;
        })
        .then(created => {
            client = createNewClient(dbParams, dbParams.keyspace);
            return client;
        })
        .catch(e => {
            // logs
            console.dir(e, { colors: true });
        });
};

const keyspaceExists = function(dbParams, client) {
    const query = `SELECT keyspace_name FROM system.schema_keyspaces WHERE keyspace_name = '${
        dbParams.keyspace
    }'`;

    return client
        .execute(query)
        .then(result => {
            if (typeof result === "undefined") {
                return false;
            }
            return true;
        })
        .catch(e => {
            // logs
            console.dir(e, { colors: true });
        });
};

const createKeyspace = function(dbParams) {
    let client = createNewClient(dbParams);

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

    client
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
return initConnection(sourceDatabase)
    .then(sourceClient => {
        data.sourceClient = sourceClient;
        return initConnection(targetDatabase);
    })
    .then(targetClient => {
        data.targetClient = targetClient;
        // select everything that describes the tenant
        // We're copying over tables: Tenant, TenantNetwork and TenantNetworkTenants
        let query = `select * from "Tenant" where "alias" = ?`;
        return data.sourceClient.execute(query, [sourceDatabase.tenantAlias]);
    })
    .then(result => {
        let row = result.first();
        return row;
    })
    .then(row => {
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
    .then(result => {
        process.exit(0);
    })
    .catch(e => {
        console.dir(e);
        process.exit(-1);
    });
