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
const path = require("path");
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

const { copyTenantTable, copyTenantConfig } = require("./tenants/dao.js");

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
    copyRevisions,
    copyEtherpadContent
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

let fileContents = fs.readFileSync(path.join(__dirname, "source.json"));
const { sourceDatabase, sourceFileHost } = JSON.parse(fileContents);

fileContents = fs.readFileSync(path.join(__dirname, "target.json"));
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

const runDatabaseCopy = async function(source, destination) {
    let copyTenant = copyTenantTable(source, destination);
    let copyConfig = copyTenantConfig(source, destination);

    (async () => {
        Promise.all([copyTenant, copyConfig])
            .then
            // log something here
            ();
    })();

    await copyAllPrincipals(source, destination);
    await copyPrincipalsByEmail(source, destination);
    await copyAuthzMembers(source, destination);
    await copyAuthzRoles(source, destination);
    await copyAllFolders(source, destination);
    await copyFoldersGroupIds(source, destination);
    await copyAllContent(source, destination);
    await copyRevisionByContent(source, destination);
    await copyRevisions(source, destination);
    await copyEtherpadContent(source, destination);
    await copyDiscussions(source, destination);
    await copyMessageBoxMessages(source, destination);
    await copyMessages(source, destination);
    await copyMessageBoxMessagesDeleted(source, destination);
    await copyMessageBoxRecentContributions(source, destination);
    await copyUsersGroupVisits(source, destination);
    await copyFollowingUsersFollowers(source, destination);
    await copyFollowingUsersFollowing(source, destination);
    await copyAuthenticationUserLoginId(source, destination);
    await copyAuthenticationLoginId(source, destination);
    await copyOAuthClientsByUser(source, destination);
    await copyOAuthClients(source, destination);
    await copyAuthzInvitations(source, destination);
    await copyAuthzInvitationsResourceIdByEmail(source, destination);
    await copyAuthzInvitationsTokenByEmail(source, destination);
    await copyAuthzInvitationsEmailByToken(source, destination);
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
        await rsync.transferFiles(source, target, contentTypes);
        await rsync.transferAssets(source, target);

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
