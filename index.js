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
      console.log('Copying tenant resources');
 * permissions and limitations under the License.
 */

const fs = require("fs");
const path = require("path");
const chalk = require("chalk");

const logger = require("./logger");
let { Store } = require("./store");
const { initConnection } = require("./db");
const rsync = require("./rsync");
const createSchemaQueries = require("./schema");

const { copyPrincipals, copyPrincipalsByEmail } = require("./principals/dao");

const { copyFolders, copyFoldersGroupIds } = require("./folders/dao");

const { copyTenant, copyTenantConfig } = require("./tenants/dao");

const {
  copyDiscussions,
  copyMessageBoxMessages,
  copyMessageBoxMessagesDeleted,
  copyMessageBoxRecentContributions,
  copyMessages
} = require("./messages/dao");

const {
  copyContent,
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

const runDatabaseCopy = async function(...args) {
  // Some of these are run one after the other, some are done concurrently
  // the ones done isolated are the ones that set variables which other queries depend on
  // all the others are just ran at the same time, so to maximize throughput

  async function copyTenantDataAndConfig(...args) {
    await Promise.all([copyTenant(...args), copyTenantConfig(...args)]);
  }

  async function copyTenantPrincipals(...args) {
    await copyPrincipals(...args);
    await copyPrincipalsByEmail(...args);
  }

  async function copyTenantResources(...args) {
    await Promise.all([copyAuthzMembers(...args), copyAuthzRoles(...args)]);
  }

  async function copyTenantFolders(...args) {
    await copyFolders(...args);
    await copyFoldersGroupIds(...args);
  }

  async function copyTenantContent(...args) {
    await Promise.all([copyContent(...args), copyEtherpadContent(...args)]);
    await copyRevisionByContent(...args);
    await copyRevisions(...args);
  }

  async function copyTenantDiscussions(...args) {
    await Promise.all([copyDiscussions(...args), copyMessages(...args)]);
    await Promise.all([
      copyMessageBoxMessages(...args),
      copyMessageBoxMessagesDeleted(...args),
      copyMessageBoxRecentContributions(...args)
    ]);
  }

  async function copyTenantGroupsAndFollowers(...args) {
    await Promise.all([
      copyUsersGroupVisits(...args),
      copyFollowingUsersFollowers(...args),
      copyFollowingUsersFollowing(...args)
    ]);
  }

  async function copyTenantAuthenticationSettings(...args) {
    await Promise.all([
      await copyOAuthClientsByUser(...args),
      await copyAuthenticationUserLoginId(...args)
    ]);
    await Promise.all([
      await copyOAuthClients(...args),
      await copyAuthenticationLoginId(...args)
    ]);
  }

  async function copyTenantInvitations(...args) {
    await copyAuthzInvitations(...args);
    await Promise.all([
      copyAuthzInvitationsResourceIdByEmail(...args),
      copyAuthzInvitationsTokenByEmail(...args)
    ]);

    await copyAuthzInvitationsEmailByToken(...args);
  }

  await copyTenantDataAndConfig(...args);
  await copyTenantPrincipals(...args);
  await copyTenantResources(...args);
  await copyTenantContent(...args);
  await copyTenantDiscussions(...args);
  await copyTenantGroupsAndFollowers(...args);
  await copyTenantAuthenticationSettings(...args);
  await copyTenantInvitations(...args);
  await copyTenantInvitations(...args);
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
    let contentTypes = ["c", "f", "u", "g"];
    await rsync.transferFiles(source, target, contentTypes);
    await rsync.transferAssets(source, target);

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