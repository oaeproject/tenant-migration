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

const fs = require("fs");
const chalk = require("chalk");

const logger = require("./logger");
let { Store } = require("./store");
const { initConnection } = require("./db");
const rsync = require("./rsync");
const createSchemaQueries = require("./schema.js");

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

new Store();
Store.init();

let fileContents = fs.readFileSync("source.json");
const { sourceDatabase, sourceFileHost } = JSON.parse(fileContents);

fileContents = fs.readFileSync("target.json");
const { targetDatabase, targetFileHost } = JSON.parse(fileContents);

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

const runDatabaseCopy = async function(source, target) {
    await copyAllTenants(source, target);
    await copyTenantConfig(source, target);
    await copyAllPrincipals(source, target);
    await copyPrincipalsByEmail(source, target);
    await copyAuthzMembers(source, target);
    await copyAuthzRoles(source, target);
    await copyAllFolders(source, target);
    await copyFoldersGroupIds(source, target);
    await copyAllContent(source, target);
    await copyRevisionByContent(source, target);
    await copyRevisions(source, target);
    await copyDiscussions(source, target);
    await copyMessageBoxMessages(source, target);
    await copyMessages(source, target);
    await copyMessageBoxMessagesDeleted(source, target);
    await copyMessageBoxRecentContributions(source, target);
    await copyUsersGroupVisits(source, target);
    await copyFollowingUsersFollowers(source, target);
    await copyFollowingUsersFollowing(source, target);
    await copyAuthenticationUserLoginId(source, target);
    await copyAuthenticationLoginId(source, target);
    await copyOAuthClientsByUser(source, target);
    await copyOAuthClients(source, target);
    await copyAuthzInvitations(source, target);
    await copyAuthzInvitationsResourceIdByEmail(source, target);
    await copyAuthzInvitationsTokenByEmail(source, target);
    await copyAuthzInvitationsEmailByToken(source, target);
};

const init = async function() {
    try {
        const sourceClient = await initConnection(sourceDatabase);
        let source = {
            database: sourceDatabase,
            client: sourceClient,
            fileHost: sourceFileHost
        };

        const targetClient = await initConnection(targetDatabase);
        let target = {
            database: targetDatabase,
            client: targetClient,
            fileHost: targetFileHost
        };

        await makeSureTablesExistOnTarget(targetClient, createSchemaQueries);
        await runDatabaseCopy(source, target);

        // Rsync the files
        let contentTypes = ["c", "f", "u", "g", "d"];
        // TODO change here: remove next line, which removes the document type
        contentTypes = ["c", "f", "u", "g"];
        await rsync.runTransfer(source, target, contentTypes);

        // TODO: rsync for assets

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
