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

const path = require("path");
const _ = require("underscore");
const chalk = require("chalk");
const { ConnectionPool } = require("ssh-pool");
const mkdirp = require("mkdirp2");

const store = require("./store");
const logger = require("./logger");

const runTransfer = async function(
    sourceFileHost,
    targetFileHost,
    contentTypes
) {
    const foldersToSync = _.map(contentTypes, eachContentType => {
        return path.join(
            "files",
            eachContentType,
            store.sourceDatabase.tenantAlias
        );
    });
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
        const rsyncData = {
            source: { directory: sourceDirectory, host: sourceFileHost.host },
            local: { directory: localDirectory, host: "localhost" },
            remote: { directory: remoteDirectory, host: targetFileHost.host }
        };
        await runEachTransfer(sourceHost, targetHost, rsyncData);
    }
};

const runEachTransfer = async function(sourceHost, targetHost, rsyncData) {
    logger.info(
        chalk.cyan(`﹅  Rsync operation under way, this may take a while...`)
    );
    logger.info(
        chalk.cyan(`﹅  Source directory:`) + ` ${rsyncData.source.directory}`
    );
    logger.info(
        chalk.cyan(`﹅  Local directory:`) + ` ${rsyncData.local.directory}`
    );
    logger.info(
        chalk.cyan(`﹅  Target directory:`) + ` ${rsyncData.remote.directory}`
    );

    logger.info(
        `${chalk.green(`✓`)}  Syncing ${chalk.cyan(
            rsyncData.source.directory
        )} on ${rsyncData.source.host} with ${chalk.cyan(
            rsyncData.local.directory
        )} on localhost`
    );
    await sourceHost.copyFromRemote(
        rsyncData.source.directory,
        rsyncData.local.directory,
        {
            verbosityLevel: 3
        }
    );

    logger.info(
        `${chalk.green(`✓`)}  Syncing ${chalk.cyan(
            rsyncData.local.directory
        )} on localhost with ${chalk.cyan(rsyncData.remote.directory)} on ${
            rsyncData.remote.host
        }`
    );
    await targetHost.copyToRemote(
        rsyncData.local.directory,
        rsyncData.remote.directory,
        {
            verbosityLevel: 3
        }
    );
    logger.info(`${chalk.green(`✓`)}  Complete!`);
};

module.exports = {
    runTransfer
};
