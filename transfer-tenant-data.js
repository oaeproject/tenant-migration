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
// [ ] promisify some bits

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

const initConnection = function(dbParams, callback) {
    callback = callback || function() {};
    client = createNewClient(dbParams, dbParams.keyspace);
    client.connect(err => {
        keyspaceExists(dbParams, client, function(err, exists) {
            if (!exists) {
                createKeyspace(dbParams, err => {
                    if (err) {
                        console.log("Unable to create keyspace: " + err);
                    }

                    client = createNewClient(dbParams, dbParams.keyspace);
                    return callback(null, client);
                });
            } else {
                return callback(null, client);
            }
        });
    });
};

const keyspaceExists = function(dbParams, client, callback) {
    const query = `SELECT keyspace_name FROM system.schema_keyspaces WHERE keyspace_name = '${
        dbParams.keyspace
    }'`;

    client.execute(query, function(err, results) {
        if (typeof results === "undefined") {
            return callback(null, false);
        } else if (err) {
            log().error(
                { err: err, name: dbParams.keyspace },
                "Error while describing cassandra keyspace"
            );
            callback({
                code: 500,
                msg: "Error while describing cassandra keyspace"
            });
        }
        return callback(null, true);
    });
};

const createKeyspace = function(dbParams, callback) {
    callback = callback || function() {};
    let keyspaceToCreate = dbParams.keyspace;
    delete dbParams.keyspace;
    let client = createNewClient(dbParams);
    dbParams.keyspace = keyspaceToCreate;

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

    client.execute(query, (err, result) => {
        if (err) {
            return callback(err);
        }

        console.log(
            `√ Created keyspace ${dbParams.keyspace} on target server ${
                dbParams.host
            }`
        );
        // pause for a second to ensure the keyspace gets agreed upon across the cluster.
        setTimeout(callback, 1000, null, true);
    });
};

initConnection(sourceDatabase, (err, sourceClient) => {
    initConnection(targetDatabase, (err, targetClient) => {
        // select everything that describes the tenant
        // We're copying over tables: Tenant, TenantNetwork and TenantNetworkTenants
        let query = `select * from "Tenant" where "alias" = ?`;
        sourceClient
            .stream(query, [sourceDatabase.tenantAlias])
            .on("readable", function() {
                // 'readable' is emitted as soon a row is received and parsed
                let row;
                while ((row = this.read())) {
                    console.log("Just fetched: \n");
                    console.dir(row, { colors: true });

                    let insertQuery = `INSERT into "Tenant" ("alias", "active", "countryCode", "displayName", "emailDomains", "host") VALUES (?, ?, ?, ?, ?, ?)`;
                    // newClient.execute(insertQuery, []);
                }
            })
            .on("end", function() {
                // Stream ended, there aren't any more rows
                console.log("Stream ended, there aren't any more rows");
                process.exit(0);
            })
            .on("error", function(err) {
                // Something went wrong: err is a response error from Cassandra
                console.log("Something went wrong: \n" + err);
            });
    });
});
