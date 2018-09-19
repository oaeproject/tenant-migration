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
const { Store } = require("../store");
const util = require("../util");
const redis = require("redis");
const { promisify } = require("util");

const ALL_PADS = "all_pads";
const ALL_GROUPS = "all_groups";
const ALL_AUTHORS = "all_authors";

const clientOptions = {
  fetchSize: 999999,
  prepare: true
};

const insertAllContent = async function(target, data, insertQuery) {
  if (_.isEmpty(data.rows)) {
    return;
  }
  await (async function insertAll(targetClient, rows) {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      await targetClient.execute(
        insertQuery,
        [
          row.contentId,
          row.created,
          row.createdBy,
          row.description,
          row.displayName,
          row.etherpadGroupId,
          row.etherpadPadId,
          row.filename,
          row.largeUri,
          row.lastModified,
          row.latestRevisionId,
          row.link,
          row.mediumUri,
          row.mime,
          row.previews,
          row.resourceSubType,
          row.size,
          row.smallUri,
          row.status,
          row.tenantAlias,
          row.thumbnailUri,
          row.uri,
          row.visibility,
          row.wideUri
        ],
        clientOptions
      );
    }
  })(target.client, data.rows);
};

const fetchAllContent = async function(target, query) {
  let allResourceIds = _.uniq(Store.getAttribute("allResourceIds"));
  let result = await target.client.execute(
    query,
    [target.database.tenantAlias],
    clientOptions
  );
  logger.info(
    `${chalk.green(`✓`)}  Fetched ${result.rows.length} Content rows...`
  );

  let allContentIds = _.pluck(result.rows, "contentId");
  Store.setAttribute("allContentIds", allContentIds);
  Store.setAttribute("allTenancyContents", result.rows);

  return result;
};

const copyContent = async function(source, destination) {
  const query = `
      SELECT *
      FROM "Content"
      WHERE "tenantAlias" = ?
      LIMIT ${clientOptions.fetchSize}`;
  const insertQuery = `
      INSERT INTO "Content" (
          "contentId",
          created,
          "createdBy",
          description,
          "displayName",
          "etherpadGroupId",
          "etherpadPadId",
          filename,
          "largeUri",
          "lastModified",
          "latestRevisionId",
          link,
          "mediumUri",
          mime,
          previews,
          "resourceSubType",
          size,
          "smallUri",
          status,
          "tenantAlias",
          "thumbnailUri",
          uri,
          visibility,
          "wideUri")
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

  let fetchedRows = await fetchAllContent(source, query);
  await insertAllContent(destination, fetchedRows, insertQuery);
  let insertedRows = await fetchAllContent(destination, query);
  util.compareResults(fetchedRows.rows.length, insertedRows.rows.length);
};

const insertAllRevisionByContent = async function(target, data, insertQuery) {
  if (_.isEmpty(data.rows)) {
    return;
  }
  await (async function insertAll(targetClient, rows) {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      await targetClient.execute(
        insertQuery,
        [row.contentId, row.created, row.revisionId],
        clientOptions
      );
    }
  })(target.client, data.rows);
};

const fetchAllRevisionByContent = async function(target, query) {
  let result = await target.client.execute(
    query,
    [Store.getAttribute("allContentIds")],
    clientOptions
  );
  let allRevisionIds = _.pluck(result.rows, "revisionId");
  Store.setAttribute("allRevisionIds", _.uniq(allRevisionIds));

  logger.info(
    `${chalk.green(`✓`)}  Fetched ${
      result.rows.length
    } RevisionByContent rows...`
  );

  return result;
};

const copyRevisionByContent = async function(source, destination) {
  const query = `
      SELECT *
      FROM "RevisionByContent"
      WHERE "contentId"
      IN ?
      LIMIT ${clientOptions.fetchSize}`;
  const insertQuery = `
      INSERT INTO "RevisionByContent" (
      "contentId",
      created,
      "revisionId")
      VALUES (?, ?, ?)`;

  let fetchedRows = await fetchAllRevisionByContent(source, query);
  await insertAllRevisionByContent(destination, fetchedRows, insertQuery);
  let insertedRows = await fetchAllRevisionByContent(destination, query);
  util.compareResults(fetchedRows.rows.length, insertedRows.rows.length);
};

const insertAllRevisions = async function(target, data, insertQuery) {
  if (_.isEmpty(data.rows)) {
    return;
  }
  await (async function insertAll(targetClient, rows) {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      await targetClient.execute(
        insertQuery,
        [
          row.revisionId,
          row.contentId,
          row.created,
          row.createdBy,
          row.etherpadHtml,
          row.filename,
          row.largeUri,
          row.mediumUri,
          row.mime,
          row.previews,
          row.previewsId,
          row.size,
          row.smallUri,
          row.status,
          row.thumbnailUri,
          row.uri,
          row.wideUri
        ],
        clientOptions
      );
    }
  })(target.client, data.rows);
};

const fetchAllRevisions = async function(target, query) {
  let result = await target.client.execute(
    query,
    [Store.getAttribute("allRevisionIds")],
    clientOptions
  );
  logger.info(
    `${chalk.green(`✓`)}  Fetched ${result.rows.length} Revisions rows...`
  );

  return result;
};

const copyRevisions = async function(source, destination) {
  const query = `
      SELECT *
      FROM "Revisions"
      WHERE "revisionId"
      IN ? LIMIT ${clientOptions.fetchSize}`;
  const insertQuery = `
      INSERT INTO "Revisions" (
          "revisionId",
          "contentId",
          created,
          "createdBy",
          "etherpadHtml",
          filename,
          "largeUri",
          "mediumUri",
          mime,
          previews,
          "previewsId",
          size,
          "smallUri",
          status,
          "thumbnailUri",
          uri,
          "wideUri")
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

  let fetchedRows = await fetchAllRevisions(source, query);
  await insertAllRevisions(destination, fetchedRows, insertQuery);
  let insertedRows = await fetchAllRevisions(destination, query);
  util.compareResults(fetchedRows.rows.length, insertedRows.rows.length);
};

const copyEtherpadContent = async function(source, target) {
  client = redis.createClient({ host: "localhost" });
  const redisHGET = promisify(client.hget).bind(client);
  const redisHLEN = promisify(client.hlen).bind(client);
  const redisHKEYS = promisify(client.hkeys).bind(client);
  const redisHVALS = promisify(client.hvals).bind(client);

  client.on("error", function(err) {
    logger.error(`${chalk.red(`✗`)}  Something went wrong: `);
    logger.error(error.stack);
  });

  let allTenancyContents = Store.getAttribute("allTenancyContents");

  let allEtherpadPadIds = _.chain(allTenancyContents)
    .pluck("etherpadPadId")
    .uniq()
    .compact()
    .value();
  let allEtherpadGroupIds = _.chain(allTenancyContents)
    .pluck("etherpadGroupId")
    .uniq()
    .compact()
    .value();

  let allPrincipalIds = Store.getAttribute("tenantPrincipals");
  // TODO might need to filter these and later add them to the content to filter with
  let allMovedResources = Store.getAttribute("movedResources");

  const query = `
      SELECT *
      FROM "Etherpad"
      LIMIT ${clientOptions.fetchSize * 100}`;
  const insertQuery = `INSERT INTO "Etherpad" (key, data) VALUES (?, ?)`;
  let counter = 0;
  let allRows = [];

  let allPads = [];
  let allGroups = [];
  let allAuthors = [];
  let allAuthorMappings = [];
  let allGroupMappings = [];

  // experimental
  let allTokens = [];
  let ueberdbs = [];
  let author2sessions = [];
  let group2sessions = [];
  let token2authors = [];
  let readonly2pads = [];
  let pad2readonlys = [];

  async function filterAuthors(allAuthorMappings) {
    let mappedAuthorsKeys = _.map(allAuthorMappings, eachAuthorMapping => {
      return `globalAuthor:${eachAuthorMapping.data.slice(1, -1)}`;
    });
    let authorsToCopy = [];
    for (let index = 0; index < allAuthorMappings.length; index++) {
      let eachMappedAuthorKey = mappedAuthorsKeys[index];
      let eachMappedAuthorValue = await redisHGET(
        ALL_AUTHORS,
        eachMappedAuthorKey
      );
      authorsToCopy.push({
        key: eachMappedAuthorKey,
        data: eachMappedAuthorValue
      });
    }
    let allAuthorsSize = await redisHLEN(ALL_AUTHORS);
    logger.info(
      `${chalk.green(`✓`)}  Selected ${
        authorsToCopy.length
      } Etherpad globalAuthor rows from a total of ${allAuthorsSize}...`
    );

    return authorsToCopy;
  }

  async function filterGroups(allGroupMappings) {
    let mappedGroupKeys = _.map(allGroupMappings, eachGroupMapping => {
      return `group:${eachGroupMapping.data.slice(1, -1)}`;
    });
    // let mappedGroupValues = await redisHMGET(ALL_GROUPS, mappedGroupKeys);
    let groupsToCopy = [];
    for (let index = 0; index < allGroupMappings.length; index++) {
      let eachMappedGroupKey = mappedGroupKeys[index];
      let eachMappedGroupValue = await redisHGET(
        ALL_GROUPS,
        eachMappedGroupKey
      );
      groupsToCopy.push({
        key: eachMappedGroupKey,
        data: eachMappedGroupValue
      });
    }
    let allGroupsSize = await redisHLEN(ALL_GROUPS);
    logger.info(
      `${chalk.green(`✓`)}  Selected ${
        groupsToCopy.length
      } Etherpad group rows from a total of ${allGroupsSize}...`
    );

    return groupsToCopy;
  }

  async function filterPads() {
    let allPadKeys = await redisHKEYS(ALL_PADS);
    let allPadsSize = await redisHLEN(ALL_PADS);
    let allPadValues = await redisHVALS(ALL_PADS);

    let uniquePadKeys = _.map(allPadKeys, eachPad => {
      return eachPad.split(":")[1];
    });
    uniquePadKeys = _.uniq(uniquePadKeys);

    let allPadsToCopy = [];
    for (let index = 0; index < allPadsSize; index++) {
      allPadsToCopy.push({
        key: allPadKeys[index],
        data: allPadValues[index]
      });
    }
    logger.info(
      `${chalk.green(`✓`)}  Selected ${
        allPadsToCopy.length
      } Etherpad pad rows from ${allPadsSize}, ${
        uniquePadKeys.length
      } are unique!`
    );
    return allPadsToCopy;
  }

  let isArray = function(a) {
    return !!a && a.constructor === Array;
  };

  async function insertRows(targetClient, rows) {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      await targetClient.execute(
        insertQuery,
        [row.key, row.data],
        clientOptions
      );
    }
  }

  const filterRows = function() {
    let row;
    while ((row = this.read())) {
      counter++;
      if (row.key.startsWith("mapper2group:")) {
        let alias = row.key.split(":")[2];
        if (alias === source.database.tenantAlias) {
          allGroupMappings.push(row);
        }
      } else if (row.key.startsWith("mapper2author:")) {
        let alias = row.key.split(":")[2];
        if (alias === source.database.tenantAlias) {
          allAuthorMappings.push(row);
        }
      } else if (row.key.startsWith("pad:")) {
        let eachPadId = row.key.slice(4);
        eachPadId = eachPadId.split(":")[0];
        if (_.contains(allEtherpadPadIds, eachPadId)) {
          // allPads.push({ key: row.key, data: row.data });
          client.hset(ALL_PADS, row.key, row.data);
        }

        // experimental
        let eachPadContentId = eachPadId.split("$")[1];
        if (eachPadContentId) {
          eachPadContentId = eachPadContentId.split("_").join(":");
        }
        if (_.contains(allMovedResources, eachPadContentId)) {
          // we found a pad that belongs to another tenant but should be copied all the same (has at least one principal managing it)
          counter++;
          client.hset(ALL_PADS, row.key, row.data);

          // debug
          //   console.log("This pad is a moved resource -> " + eachPadId);
        }
      } else if (row.key.startsWith("group:")) {
        let eachGoupId = row.key.split(":")[1];
        if (_.contains(allEtherpadGroupIds, eachGoupId)) {
          // allGroups.push({ key: row.key, data: row.data });
          client.hset(ALL_GROUPS, row.key, row.data);
        }
      } else if (row.key.startsWith("globalAuthor:")) {
        // AllAuthors.push({ key: row.key, data: row.data });
        client.hset(ALL_AUTHORS, row.key, row.data);
      }
      /*
            else if (row.key.startsWith("token2author:")) {
                allTokens.push("globalAuthor:" + row.data.slice(1, -1));
                token2authors.push(row);
            } else if (row.key.startsWith("session:")) {
                allSessions.push(row);
            } else if (row.key.startsWith("sessionstorage:")) {
                allSessionStorages.push(row);
            } else if (row.key.startsWith("ueberdb:")) {
                ueberdbs.push(row);
            } else if (row.key.startsWith("author2sessions:")) {
                author2sessions.push(row);
            } else if (row.key.startsWith("group2sessions:")) {
                group2sessions.push(row);
            } else if (row.key.startsWith("readonly2pad:")) {
                readonly2pads.push(row);
            } else if (row.key.startsWith("pad2readonly")) {
                pad2readonlys.push(row);
            }
            */
    }
  };

  const afterQuery = async function() {
    logger.info(`${chalk.green(`✓`)}  Filtered ${counter} Etherpad rows...`);

    let authorsToCopy = await filterAuthors(allAuthorMappings);
    logger.info(
      `${chalk.green(`✓`)}  Inserting ${
        authorsToCopy.length
      } Etherpad globalAuthor rows...`
    );
    await insertRows(target.client, authorsToCopy);

    let groupsToCopy = await filterGroups(allGroupMappings);
    logger.info(
      `${chalk.green(`✓`)}  Inserting ${
        groupsToCopy.length
      } Etherpad group rows...`
    );
    await insertRows(target.client, groupsToCopy);

    logger.info(
      `${chalk.green(`✓`)}  Inserting ${
        allGroupMappings.length
      } Etherpad mapper2group rows...`
    );
    await insertRows(target.client, allGroupMappings);

    logger.info(
      `${chalk.green(`✓`)}  Inserting ${
        allAuthorMappings.length
      } Etherpad mapper2author rows...`
    );
    await insertRows(target.client, allAuthorMappings);

    let padsToCopy = await filterPads();
    logger.info(
      `${chalk.green(`✓`)}  Inserting ${padsToCopy.length} Etherpad pad rows...`
    );
    await insertRows(target.client, padsToCopy);

    /*
                allRows = _.union(
                    allAuthorMappings,
                    allGroupMappings,
                    padsToCopy,
                    authorsToCopy,
                    groupsToCopy

                    // author2sessions,
                    // group2sessions,

                    // ueberdbs,
                    // token2authors,
                    // pad2readonlys,
                    // readonly2pads
                );

                if (_.isEmpty(allRows)) {
                    // return;
                    resolve([]);
                }
            */
  };

  function streamRows() {
    const com = source.client.stream(query);
    const p = new Promise((resolve, reject) => {
      com.on("end", async () => {
        await afterQuery();
        resolve();
      });
      com.on("error", reject);
    });
    p.on = function() {
      com.on.apply(com, arguments);
      return p;
    };
    return p;
  }
  return streamRows().on("readable", filterRows);
};

module.exports = {
  copyContent,
  copyRevisionByContent,
  copyRevisions,
  copyEtherpadContent
};
