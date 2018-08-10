#! /bin/node
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

// @format
const fs = require("fs");
const path = require("path");
const _ = require("underscore");
const chalk = require("chalk");

const logger = require("./logger");
const { initConnection } = require("./db");
const createSchemaQueries = require("./schema.js");
const store = require("./store");
const rsync = require("./rsync");

// Read json file with source database and keyspace
let fileContents = fs.readFileSync("source.json");
const { sourceDatabase, sourceFileHost } = JSON.parse(fileContents);
store.sourceDatabase = sourceDatabase;

// Read json file with target database and keyspace
fileContents = fs.readFileSync("target.json");
const { targetDatabase, targetFileHost } = JSON.parse(fileContents);
store.targetDatabase = targetDatabase;

const {
    copyAllPrincipals,
    copyPrincipalsByEmail
} = require("./principals/dao.js");

const { copyAllFolders, copyFoldersGroupIds } = require("./folders/dao.js");

const { copyAllTenants, copyTenantConfig } = require("./tenants/dao.js");

const {
    copyDiscussions,
    copyMessageBoxMessages,
    copyMessageBoxMessagesDeleted,
    copyMessageBoxRecentContributions,
    copyMessages
} = require("./messages/dao.js");

const {
    copyAllContent,
    copyRevisionByContent,
    copyRevisions
} = require("./content/dao");

const { copyAuthzMembers, copyAuthzRoles } = require("./roles/dao");

const { copyUsersGroupVisits } = require("./groups/dao");

const {
    copyFollowingUsersFollowers,
    copyFollowingUsersFollowing
} = require("./following/dao");

const {
    copyAuthenticationLoginId,
    copyAuthenticationUserLoginId,
    copyOAuthClients,
    copyOAuthClientsByUser
} = require("./authentication/dao");

const {
    copyAuthzInvitations,
    copyAuthzInvitationsEmailByToken,
    copyAuthzInvitationsResourceIdByEmail,
    copyAuthzInvitationsTokenByEmail
} = require("./invitations/dao");

const makeSureTablesExistOnTarget = async function(
    targetClient,
    createAllTables
) {
    const allPromises = [];
    createAllTables.forEach(eachCreateStatement => {
        allPromises.push(
            Promise.resolve(targetClient.execute(eachCreateStatement.query))
        );
    });
    logger.info(`${chalk.green(`✓`)}  Creating tables...\n`);
    await Promise.all(allPromises);
};

const runDatabaseCopy = async function(sourceClient, targetClient) {
    await copyAllTenants(sourceClient, targetClient);
    await copyTenantConfig(sourceClient, targetClient);
    await copyAllPrincipals(sourceClient, targetClient);
    await copyPrincipalsByEmail(sourceClient, targetClient);
    await copyAuthzMembers(sourceClient, targetClient);
    await copyAuthzRoles(sourceClient, targetClient);
    await copyAllFolders(sourceClient, targetClient);
    await copyFoldersGroupIds(sourceClient, targetClient);
    await copyAllContent(sourceClient, targetClient);
    await copyRevisionByContent(sourceClient, targetClient);
    await copyRevisions(sourceClient, targetClient);
    await copyDiscussions(sourceClient, targetClient);
    await copyMessageBoxMessages(sourceClient, targetClient);
    await copyMessages(sourceClient, targetClient);
    await copyMessageBoxMessagesDeleted(sourceClient, targetClient);
    await copyMessageBoxRecentContributions(sourceClient, targetClient);
    await copyUsersGroupVisits(sourceClient, targetClient);
    await copyFollowingUsersFollowers(sourceClient, targetClient);
    await copyFollowingUsersFollowing(sourceClient, targetClient);
    await copyAuthenticationUserLoginId(sourceClient, targetClient);
    await copyAuthenticationLoginId(sourceClient, targetClient);
    await copyOAuthClientsByUser(sourceClient, targetClient);
    await copyOAuthClients(sourceClient, targetClient);
    await copyAuthzInvitations(sourceClient, targetClient);
    await copyAuthzInvitationsResourceIdByEmail(sourceClient, targetClient);
    await copyAuthzInvitationsTokenByEmail(sourceClient, targetClient);
    await copyAuthzInvitationsEmailByToken(sourceClient, targetClient);
};

const init = async function() {
    try {
        const sourceClient = await initConnection(sourceDatabase);
        const targetClient = await initConnection(targetDatabase);

        // Rsync the files
        let contentTypes = ["c", "f", "u", "g", "d"];
        // TODO change here: remove next line, which removes the document type
        contentTypes = ["c", "f", "u", "g"];
        await rsync.runTransfer(sourceFileHost, targetFileHost, contentTypes);

        // Start with the database data transfer
        await makeSureTablesExistOnTarget(targetClient, createSchemaQueries);
        await runDatabaseCopy(sourceClient, targetClient);

        logger.info(`${chalk.green(`✓`)}  Exiting.`);
    } catch (error) {
        logger.error(`${chalk.red(`✗`)}  Something went wrong: `);
        logger.error(error.stack);
        process.exit(-1);
    } finally {
        logger.end();
        process.exit(0);
    }
};

init();
