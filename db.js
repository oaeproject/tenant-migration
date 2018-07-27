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

const _ = require("underscore");
const cassandra = require("cassandra-driver");
const logger = require("./logger");
const chalk = require("chalk");

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

const createKeyspace = async function(dbParams, client) {
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

    try {
        let result = await client.execute(query);
        logger.info(
            `${chalk.green(`✓`)}  Created keyspace ${dbParams.keyspace} on ${
                dbParams.host
            }`
        );
        return !!result;
    } catch (error) {
        logger.error(`${chalk.red(`✗`)}  Something went wrong: ` + e);
        process.exit(-1);
    }
};

const keyspaceExists = async function(dbParams, client) {
    const query = `SELECT keyspace_name FROM system.schema_keyspaces WHERE keyspace_name = '${
        dbParams.keyspace
    }'`;

    try {
        let result = await client.execute(query);
        return !_.isEmpty(result.rows);
    } catch (error) {
        logger.error(`${chalk.red(`✗`)}  Something went wrong: ` + e);
        process.exit(-1);
    }
};

const initConnection = async function(dbParams) {
    logger.info(
        `${chalk.green(`✓`)}  Initialising connection to ${dbParams.host}/${
            dbParams.keyspace
        }`
    );

    try {
        let client = createNewClient(dbParams);
        await client.connect();
        let exists = await keyspaceExists(dbParams, client);
        if (!exists) {
            await createKeyspace(dbParams, client);
        }

        client = await createNewClient(dbParams, dbParams.keyspace);
        return client;
    } catch (error) {
        logger.error(`${chalk.red(`✗`)}  Something went wrong: ` + error);
        process.exit(-1);
    }
};

module.exports = {
    initConnection
};
