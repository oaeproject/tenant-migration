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

const _ = require("underscore");
const chalk = require("chalk");
const fs = require("fs");

const logger = require("./logger");
const { initConnection } = require("./db");
let createSchemaQueries = require("./schema.js");
let store = require("./store");

// read json file with source database and keyspace
let fileContents = fs.readFileSync("source-database.json");
const { sourceDatabase } = JSON.parse(fileContents);
store.sourceDatabase = sourceDatabase;

// read json file with target database and keyspace
fileContents = fs.readFileSync("target-database.json");
const { targetDatabase } = JSON.parse(fileContents);
store.targetDatabase = targetDatabase;

const createAllTables = async function(targetClient, createAllTables) {
    allPromises = [];
    createAllTables.forEach(eachCreateStatement => {
        allPromises.push(
            Promise.resolve(targetClient.execute(eachCreateStatement.query))
        );
    });
    logger.info(`${chalk.green(`✓`)}  Creating tables...`);
    await Promise.all(allPromises);
};

const {
    insertAllPrincipals,
    insertAllPrincipalsByEmail,
    selectAllPrincipals,
    selectPrincipalsByEmail
} = require("./principals/dao.js");

const {
    insertAllFolders,
    insertFoldersGroupIds,
    selectAllFolders,
    selectFoldersGroupIds
} = require("./folders/dao.js");

const {
    selectAllTenants,
    insertAllTenants,
    selectTenantConfig,
    insertTenantConfig
} = require("./tenants/dao.js");

const {
    insertDiscussions,
    insertMessageBoxMessages,
    insertMessageBoxMessagesDeleted,
    insertMessageBoxRecentContributions,
    insertMessages,
    selectDiscussions,
    selectMessageBoxMessages,
    selectMessageBoxMessagesDeleted,
    selectMessageBoxRecentContributions,
    selectMessages
} = require("./messages/dao.js");

const {
    selectAllContent,
    selectRevisionByContent,
    selectRevisions,
    insertContent,
    insertRevisionByContent,
    insertRevisions
} = require("./content/dao");

const {
    selectAuthzMembers,
    insertAllAuthzMembers,
    selectAuthzRoles,
    insertAuthzRoles
} = require("./roles/dao");

const {
    selectUsersGroupVisits,
    insertUsersGroupVisits
} = require("./groups/dao");

const {
    selectFollowingUsersFollowers,
    selectFollowingUsersFollowing,
    insertFollowingUsersFollowers,
    insertFollowingUsersFollowing
} = require("./following/dao");

const {
    selectAuthenticationLoginId,
    selectAuthenticationUserLoginId,
    selectOAuthClients,
    insertAuthenticationUserId,
    insertAuthenticationUserLoginId,
    insertOAuthClients,
    selectOAuthClientsByUser,
    insertOAuthClientsByUser
} = require("./authentication/dao");

const {
    selectAuthzInvitations,
    selectAuthzInvitationsEmailByToken,
    selectAuthzInvitationsResourceIdByEmail,
    selectAuthzInvitationsTokenByEmail,
    insertAuthzInvitations,
    insertAuthzInvitationsEmailByToken,
    insertAuthzInvitationsResourceIdByEmail,
    insertAuthzInvitationsTokenByEmail
} = require("./invitations/dao");

const init = async function() {
    let sourceClient = await initConnection(sourceDatabase);
    let targetClient = await initConnection(targetDatabase);

    try {
        await createAllTables(targetClient, createSchemaQueries);
        await insertAllTenants(
            targetClient,
            await selectAllTenants(sourceClient)
        );
        await insertTenantConfig(
            targetClient,
            await selectTenantConfig(sourceClient)
        );
        await insertAllPrincipals(
            targetClient,
            await selectAllPrincipals(sourceClient)
        );
        await insertAllPrincipalsByEmail(
            targetClient,
            await selectPrincipalsByEmail(sourceClient)
        );
        await insertAllAuthzMembers(
            targetClient,
            await selectAuthzMembers(sourceClient)
        );
        await insertAllFolders(
            targetClient,
            await selectAllFolders(sourceClient)
        );

        await insertFoldersGroupIds(
            targetClient,
            await selectFoldersGroupIds(sourceClient)
        );

        await insertAuthzRoles(
            targetClient,
            await selectAuthzRoles(sourceClient)
        );
        await insertContent(targetClient, await selectAllContent(sourceClient));
        await insertRevisionByContent(
            targetClient,
            await selectRevisionByContent(sourceClient)
        );
        await insertRevisions(
            targetClient,
            await selectRevisions(sourceClient)
        );
        await insertDiscussions(
            targetClient,
            await selectDiscussions(sourceClient)
        );
        await insertMessageBoxMessages(
            targetClient,
            await selectMessageBoxMessages(sourceClient)
        );
        await insertMessages(targetClient, await selectMessages(sourceClient));
        await insertMessageBoxMessagesDeleted(
            targetClient,
            await selectMessageBoxMessagesDeleted(sourceClient)
        );
        await insertMessageBoxRecentContributions(
            targetClient,
            await selectMessageBoxRecentContributions(sourceClient)
        );
        await insertUsersGroupVisits(
            targetClient,
            await selectUsersGroupVisits(sourceClient)
        );
        await insertFollowingUsersFollowers(
            targetClient,
            await selectFollowingUsersFollowers(sourceClient)
        );
        await insertFollowingUsersFollowing(
            targetClient,
            await selectFollowingUsersFollowing(sourceClient)
        );
        await insertAuthenticationUserLoginId(
            targetClient,
            await selectAuthenticationUserLoginId(sourceClient)
        );
        await insertAuthenticationUserId(
            targetClient,
            await selectAuthenticationLoginId(sourceClient)
        );
        await insertOAuthClientsByUser(
            targetClient,
            await selectOAuthClientsByUser(sourceClient)
        );
        await insertOAuthClients(
            targetClient,
            await selectOAuthClients(sourceClient)
        );
        await insertAuthzInvitations(
            targetClient,
            await selectAuthzInvitations(sourceClient)
        );
        await insertAuthzInvitationsResourceIdByEmail(
            targetClient,
            await selectAuthzInvitationsResourceIdByEmail(sourceClient)
        );
        await insertAuthzInvitationsTokenByEmail(
            targetClient,
            await selectAuthzInvitationsTokenByEmail(sourceClient)
        );
        await insertAuthzInvitationsEmailByToken(
            targetClient,
            await selectAuthzInvitationsEmailByToken(sourceClient)
        );
        logger.info(`${chalk.green(`✓`)}  Exiting.`);
    } catch (error) {
        logger.error(`${chalk.red(`✗`)}  Something went wrong: ` + error);
        console.dir(error, { colors: true });
        process.exit(-1);
    } finally {
        logger.end();
        process.exit(0);
    }
};

init();
