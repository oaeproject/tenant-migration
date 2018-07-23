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

module.exports = {
    initConnection
};
