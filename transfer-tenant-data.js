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
const { ConnectionPool } = require("ssh-pool");
const mkdirp = require("mkdirp2");

const logger = require("./logger");
const { initConnection } = require("./db");
const createSchemaQueries = require("./schema.js");
const store = require("./store");

// Read json file with source database and keyspace
let fileContents = fs.readFileSync("source.json");
const { sourceDatabase, sourceFileHost } = JSON.parse(fileContents);
store.sourceDatabase = sourceDatabase;

// Read json file with target database and keyspace
fileContents = fs.readFileSync("target.json");
const { targetDatabase, targetFileHost } = JSON.parse(fileContents);
store.targetDatabase = targetDatabase;

const createAllTables = async function(targetClient, createAllTables) {
    const allPromises = [];
    createAllTables.forEach(eachCreateStatement => {
        allPromises.push(
            Promise.resolve(targetClient.execute(eachCreateStatement.query))
        );
    });
    logger.info(`${chalk.green(`✓`)}  Creating tables...\n`);
    await Promise.all(allPromises);
};

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

async function runTransfer(sourceFileHost, targetFileHost, foldersToSync) {
    // Create the ssh connections to both origin/target servers
    const sourceHost = new ConnectionPool([
        `${sourceFileHost.user}@${sourceFileHost.host}`
    ]);
    const targetHost = new ConnectionPool([
        `${targetFileHost.user}@${targetFileHost.host}`
    ]);

    let sourceDirectory = sourceFileHost.path;
    const targetPath = targetFileHost.path;
    const localPath = process.cwd();

    /* eslint-disable no-await-in-loop */
    for (let i = 0; i < foldersToSync.length; i++) {
        const eachFolder = foldersToSync[i];

        // the origin folder that we're syncing to other servers
        sourceDirectory = path.join(sourceFileHost.path, eachFolder);

        // Make sure the directories exist locally otherwise rsync fails
        const localDirectory = path.join(localPath, eachFolder);
        await mkdirp.promise(localDirectory);

        // Make sure the directories exist remotely otherwise rsync fails
        const remoteDirectory = path.join(targetPath, eachFolder);
        await targetHost.run(`mkdir -p ${remoteDirectory}`);

        // const runEachTransfer = async function(directories, hosts) {};

        const directories = [sourceDirectory, localDirectory, remoteDirectory];
        const hosts = [sourceFileHost.host, "localhost", targetFileHost.host];
        // await runEachTransfer(directories, hosts);

        logger.info(
            chalk.cyan(
                `﹅  Rsync operation under way, this may take a while...`
            )
        );
        logger.info(
            chalk.cyan(`﹅  Source directory:`) + ` ${sourceDirectory}`
        );
        logger.info(chalk.cyan(`﹅  Local directory:`) + ` ${localDirectory}`);
        logger.info(
            chalk.cyan(`﹅  Target directory:`) + ` ${remoteDirectory}`
        );

        logger.info(
            `${chalk.green(`✓`)}  Syncing ${chalk.cyan(sourceDirectory)} on ${
                sourceFileHost.host
            } with ${chalk.cyan(localDirectory)} on localhost`
        );
        await sourceHost.copyFromRemote(sourceDirectory, localDirectory, {
            verbosityLevel: 3
        });
        logger.info(`${chalk.green(`✓`)}  Complete!\n`);

        logger.info(
            `${chalk.green(`✓`)}  Syncing ${chalk.cyan(
                localDirectory
            )} on localhost with ${chalk.cyan(remoteDirectory)} on ${
                targetFileHost.host
            }`
        );
        await targetHost.copyToRemote(localDirectory, remoteDirectory, {
            verbosityLevel: 3
        });
        logger.info(`${chalk.green(`✓`)}  Complete!\n`);
    }
}

const init = async function() {
    try {
        const sourceClient = await initConnection(sourceDatabase);
        const targetClient = await initConnection(targetDatabase);

        // Rsync the files
        let contentTypes = ["c", "f", "u", "g", "d"];
        // TODO change here: remove next line, which removes the document type
        contentTypes = ["c", "f", "u", "g"];
        const foldersToSync = _.map(contentTypes, eachContentType => {
            return path.join(
                "files",
                eachContentType,
                sourceDatabase.tenantAlias
            );
        });

        await runTransfer(sourceFileHost, targetFileHost, foldersToSync);

        // process.exit(0);

        // Start with the database data transfer
        await createAllTables(targetClient, createSchemaQueries);
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
