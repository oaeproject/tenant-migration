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

const copyAuthzRoles = async function(sourceClient, targetClient) {
    let query = `SELECT * FROM "AuthzRoles" WHERE "principalId" IN ? LIMIT 999999`;
    let insertQuery = `INSERT INTO "AuthzRoles" ("principalId", "resourceId", role) VALUES (?, ?, ?)`;
    let counter = 0;

    let result = await sourceClient.execute(
        query,
        [store.tenantPrincipals],
        clientOptions
    );
    store.allResourceIds = _.pluck(result.rows, "resourceId");

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            let row = rows[i];
            counter++;

            await targetClient.execute(
                insertQuery,
                [row.principalId, row.resourceId, row.role],
                clientOptions
            );
        }
    }

    logger.info(
        `${chalk.green(`✓`)}  Fetched ${result.rows.length} AuthzRoles rows...`
    );
    if (_.isEmpty(result.rows)) {
        return;
    }
    await insertAll(targetClient, result.rows);
    logger.info(
        `${chalk.green(`✓`)}  Inserted ${counter} AuthzRoles rows...\n`
    );
};

const copyAuthzMembers = async function(sourceClient, targetClient) {
    let query = `SELECT * FROM "AuthzMembers" WHERE "memberId" IN ? ALLOW FILTERING`;
    let insertQuery = `INSERT INTO "AuthzMembers" ("resourceId", "memberId", role) VALUES (?, ?, ?)`;
    let counter = 0;

    let result = await sourceClient.execute(
        query,
        [store.tenantPrincipals],
        clientOptions
    );

    async function insertAll(targetClient, rows) {
        for (let i = 0; i < rows.length; i++) {
            let row = rows[i];
            counter++;

            await targetClient.execute(
                insertQuery,
                [row.resourceId, row.memberId, row.role],
                clientOptions
            );
        }
    }

    logger.info(
        `${chalk.green(`✓`)}  Fetched ${
            result.rows.length
        } AuthzMembers rows...`
    );
    if (_.isEmpty(result.rows)) {
        return;
    }
    await insertAll(targetClient, result.rows);
    logger.info(
        `${chalk.green(`✓`)}  Inserted ${counter} AuthzMembers rows...\n`
    );
};

const insertAllAuthzMembers = async function(targetClient, result) {
    // insert authzmembers
    if (_.isEmpty(result.rows)) {
        logger.info(
            `${chalk.green(`✗`)}  Skipped fetching AuthzMembers rows...\n`
        );
        return;
    }

    let allInserts = [];
    result.rows.forEach(row => {
        allInserts.push(
            new Promise((resolve, reject) => {
                setTimeout(() => {
                    Promise.resolve(
                        targetClient.execute(
                            `INSERT INTO "AuthzMembers" ("resourceId", "memberId", role) VALUES (?, ?, ?)`,
                            [row.resourceId, row.memberId, row.role]
                        )
                    )
                        .then(resolve())
                        .catch(e => {
                            reject(e);
                        });
                }, 1000);
            })
        );
    });
    logger.info(`${chalk.green(`✓`)}  Inserting AuthzMembers...\n`);
    // await targetClient.batch(allInserts, { prepare: true });
    // const firstTask = allInserts.shift();

    async function runAll(allInserts) {
        for (const insertQuery of allInserts) {
            await insertQuery;
        }
    }
    console.log("Total inserts to AuthzMembers is: " + allInserts.length);
    await runAll(allInserts);
    // await Promise.all(allInserts);
};

module.exports = {
    copyAuthzMembers,
    copyAuthzRoles
};
