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

const path = require('path');
const _ = require('underscore');
const chalk = require('chalk');
const { ConnectionPool } = require('ssh-pool');
const mkdirp = require('mkdirp2');
const { Store } = require('./store');
const logger = require('./logger');

const transferFiles = async function(source, destination, contentTypes) {
  const foldersToSync = _.map(contentTypes, eachContentType => {
    return path.join('files', eachContentType);
  });
  // Create the ssh connections to both origin/target servers
  const sourceHostConnection = new ConnectionPool([`${source.files.user}@${source.files.host}`]);
  const destinationHostConnection = new ConnectionPool([
    `${destination.files.user}@${destination.files.host}`
  ]);

  let sourceDirectory = source.files.path;
  const destinationPath = destination.files.path;
  const localPath = process.cwd();

  /* eslint-disable no-await-in-loop */
  for (let i = 0; i < foldersToSync.length; i++) {
    const eachFolder = foldersToSync[i];

    // The origin folder that we're syncing to other servers
    sourceDirectory = path.join(source.files.path, eachFolder, source.database.tenantAlias);

    // Make sure the directories exist locally otherwise rsync fails
    const localDirectory = path.join(localPath, eachFolder);
    await mkdirp.promise(localDirectory);

    // Make sure the directories exist remotely otherwise rsync fails
    const remoteDirectory = path.join(destinationPath, eachFolder);
    await destinationHostConnection.run(`mkdir -p ${remoteDirectory}`);

    await runEachTransfer(
      sourceHostConnection,
      destinationHostConnection,
      {
        source: {
          directory: sourceDirectory,
          host: source.files.host
        },
        local: { directory: localDirectory, host: 'localhost' },
        remote: {
          directory: remoteDirectory,
          host: destination.files.host
        }
      },
      source.database.tenantAlias
    );
  }

  const movedResources = Store.getAttribute('movedResources');
  if (movedResources) {
    for (let i = 0; i < movedResources.length; i++) {
      const eachResource = movedResources[i];
      const eachResourceParts = eachResource.split(':');
      const [eachContentType, tenantAlias, eachResourceId] = eachResourceParts;

      // TODO we need to later include etherpad documents in this
      if (eachContentType !== 'd') {
        const eachCurrentFilePath = path.join(
          source.files.path,
          'files',
          eachContentType,
          tenantAlias,
          eachResourceId.slice(0, 2),
          eachResourceId.slice(2, 4),
          eachResourceId.slice(4, 6),
          eachResourceId.slice(6, 8)
        );
        let eachLocalFilePath = path.join(
          process.cwd(),
          'files',
          eachContentType,
          tenantAlias,
          eachResourceId.slice(0, 2),
          eachResourceId.slice(2, 4),
          eachResourceId.slice(4, 6)
        );
        logger.info(
          `${chalk.green(`✓`)}  Syncing ${chalk.cyan(eachCurrentFilePath)} on ${
            source.files.host
          } with ${chalk.cyan(eachLocalFilePath)} on localhost`
        );

        // For some reason, the file we're copying might NOT exist remotely, so we need to check beforehand
        let fileExistsOnSource = await sourceHostConnection.run(
          'test -d ' + eachCurrentFilePath + '; echo $?'
        );
        fileExistsOnSource = parseInt(_.first(fileExistsOnSource).stdout, 10);

        if (fileExistsOnSource === 0) {
          await mkdirp.promise(eachLocalFilePath);
          await sourceHostConnection.copyFromRemote(eachCurrentFilePath, eachLocalFilePath, {
            verbosityLevel: 3
          });

          eachLocalFilePath = path.join(eachLocalFilePath, eachResourceId.slice(6, 8));

          const eachRemoteFilePath = path.join(
            destinationPath,
            'files',
            eachContentType,
            source.database.tenantAlias,
            eachResourceId.slice(0, 2),
            eachResourceId.slice(2, 4),
            eachResourceId.slice(4, 6)
          );

          logger.info(
            `${chalk.green(`✓`)}  Syncing ${chalk.cyan(
              eachLocalFilePath
            )} on localhost with ${chalk.cyan(eachRemoteFilePath)} on ${destination.files.host}`
          );
          await destinationHostConnection.run(`mkdir -p ${eachRemoteFilePath}`);
          await destinationHostConnection.copyToRemote(eachLocalFilePath, eachRemoteFilePath, {
            verbosityLevel: 3
          });
        }
      }
    }
  }
};

const runEachTransfer = async function(sourceHost, destinationHost, rsyncData, tenantAlias) {
  logger.info(chalk.cyan(`﹅  Rsync operation under way, this may take a while...`));
  logger.info(chalk.cyan(`﹅  Source directory:`) + ` ${rsyncData.source.directory}`);
  logger.info(chalk.cyan(`﹅  Local directory:`) + ` ${rsyncData.local.directory}`);
  logger.info(chalk.cyan(`﹅  Target directory:`) + ` ${rsyncData.remote.directory}`);

  logger.info(
    `${chalk.green(`✓`)}  Syncing ${chalk.cyan(rsyncData.source.directory)} on ${
      rsyncData.source.host
    } with ${chalk.cyan(rsyncData.local.directory)} on localhost`
  );
  await sourceHost.copyFromRemote(rsyncData.source.directory, rsyncData.local.directory, {
    verbosityLevel: 3
  });

  logger.info(
    `${chalk.green(`✓`)}  Syncing ${chalk.cyan(
      rsyncData.local.directory
    )} on localhost with ${chalk.cyan(rsyncData.remote.directory)} on ${rsyncData.remote.host}`
  );
  await destinationHost.copyToRemote(
    path.join(rsyncData.local.directory, tenantAlias),
    rsyncData.remote.directory,
    {
      verbosityLevel: 3
    }
  );
  logger.info(`${chalk.green(`✓`)}  Complete!`);
};

const transferAssets = async function(source, target) {
  // Create the ssh connections to both origin/target servers
  const sourceHostConnection = new ConnectionPool([`${source.files.user}@${source.files.host}`]);
  const targetHostConnection = new ConnectionPool([`${target.files.user}@${target.files.host}`]);

  const sourceAssetsPath = path.join('/shared/assets', source.database.tenantAlias);
  const localAssetsPath = path.join(process.cwd(), 'assets');
  const targetAssetsPath = path.join(target.files.path, 'assets');

  logger.info(
    `${chalk.green(`✓`)}  Syncing ${chalk.cyan(sourceAssetsPath)} on ${
      source.files.host
    } with ${chalk.cyan(localAssetsPath)} on localhost`
  );
  await sourceHostConnection.copyFromRemote(sourceAssetsPath, localAssetsPath, {
    verbosityLevel: 3
  });

  logger.info(
    `${chalk.green(`✓`)}  Syncing ${chalk.cyan(
      path.join(localAssetsPath, source.database.tenantAlias)
    )} on localhost with ${chalk.cyan(targetAssetsPath)} on ${target.files.host}`
  );
  await targetHostConnection.run(`mkdir -p ${targetAssetsPath}`);
  await targetHostConnection.copyToRemote(
    path.join(localAssetsPath, source.database.tenantAlias),
    targetAssetsPath,
    {
      verbosityLevel: 3
    }
  );
};

module.exports = {
  transferFiles,
  transferAssets
};
