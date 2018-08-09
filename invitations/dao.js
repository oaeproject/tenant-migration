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

const chalk = require('chalk');
const _ = require('underscore');
const logger = require('../logger');
const store = require('../store');
const util = require('../util');

const clientOptions = {
	fetchSize: 999999,
	prepare: true
};

const copyAuthzInvitations = async function (sourceClient, targetClient) {
	const query = `SELECT * FROM "AuthzInvitations" WHERE "resourceId" IN ? LIMIT ${
		clientOptions.fetchSize
	}`;
	const insertQuery = `INSERT INTO "AuthzInvitations" ("resourceId", email, "inviterUserId", role) VALUES (?, ?, ?, ?)`;
	let counter = 0;

	let result = await sourceClient.execute(
        query,
        [store.allResourceIds],
        clientOptions
    );
	store.allInvitationEmails = _.pluck(result.rows, 'email');

	async function insertAll(targetClient, rows) {
		for (let i = 0; i < rows.length; i++) {
			const row = rows[i];
			counter++;

			await targetClient.execute(
                insertQuery,
                [row.resourceId, row.email, row.inviterUserId, row.role],
                clientOptions
            );
		}
	}

    logger.info(
        `${chalk.green(`✓`)}  Fetched ${
        	result.rows.length
        } AuthzInvitations rows found...`
    );
    if (_.isEmpty(result.rows)) {
    	return;
    }
    await insertAll(targetClient, result.rows);
    logger.info(
        `${chalk.green(`✓`)}  Inserted ${counter} AuthzInvitations rows..`
    );

    const queryResultOnSource = result;
    result = await targetClient.execute(
        query,
        [store.allResourceIds],
        clientOptions
    );
    util.compareBothTenants(
        queryResultOnSource.rows.length,
        result.rows.length
    );
};

const copyAuthzInvitationsResourceIdByEmail = async function (
	sourceClient,
	targetClient
) {
	if (_.isEmpty(store.allInvitationEmails)) {
        logger.info(
            chalk.cyan(
                `✗  Skipped fetching AuthzInvitationsResourceIdByEmail rows...\n`
            )
        );
        return [];
	}

	const query = `SELECT * FROM "AuthzInvitationsResourceIdByEmail" WHERE email IN ? LIMIT ${
		clientOptions.fetchSize
	}`;
	const insertQuery = `INSERT INTO "AuthzInvitationsResourceIdByEmail" (email, "resourceId") VALUES (?, ?)`;
	let counter = 0;

	let result = await sourceClient.execute(
        query,
        [store.allInvitationEmails],
        clientOptions
    );

	async function insertAll(targetClient, rows) {
		for (let i = 0; i < rows.length; i++) {
			const row = rows[i];
			counter++;

			await targetClient.execute(
                insertQuery,
                [row.email, row.resourceId],
                clientOptions
            );
		}
	}
    logger.info(
        `${chalk.green(`✓`)}  Fetched ${
        	result.rows.length
        } AuthzInvitationsResourceIdByEmail rows found...`
    );
    if (_.isEmpty(result.rows)) {
    	return;
    }

    await insertAll(targetClient, result.rows);
    logger.info(
        `${chalk.green(
            `✓`
        )}  Inserted ${counter} AuthzInvitationsResourceIdByEmail rows...`
    );

    const queryResultOnSource = result;
    result = await targetClient.execute(
        query,
        [store.allInvitationEmails],
        clientOptions
    );
    util.compareBothTenants(
        queryResultOnSource.rows.length,
        result.rows.length
    );
};

const copyAuthzInvitationsTokenByEmail = async function (
	sourceClient,
	targetClient
) {
	if (_.isEmpty(store.allInvitationEmails)) {
        logger.info(
            chalk.cyan(
                `✗  Skipped fetching AuthzInvitationsTokenByEmail rows...\n`
            )
        );
        return [];
	}
	const query = `SELECT * FROM "AuthzInvitationsTokenByEmail" WHERE email IN ? LIMIT ${
		clientOptions.fetchSize
	}`;
	const insertQuery = `INSERT INTO "AuthzInvitationsTokenByEmail" (email, "token") VALUES (?, ?)`;
	let counter = 0;

	result = await targetClient.execute(
        query,
        [store.allInvitationEmails],
        clientOptions
    );
	store.allInvitationTokens = _.pluck(result.rows, 'token');

	async function insertAll(targetClient, rows) {
		for (let i = 0; i < rows.length; i++) {
			const row = rows[i];
			counter++;

			await targetClient.execute(
                insertQuery,
                [row.email, row.token],
                clientOptions
            );
		}
	}
    logger.info(
        `${chalk.green(`✓`)}  Fetched ${
        	result.rows.length
        } AuthzInvitationsTokenByEmail rows...`
    );
    if (_.isEmpty(result.rows)) {
    	return;
    }

    await insertAll(targetClient, result.rows);
    logger.info(
        `${chalk.green(
            `✓`
        )}  Inserted ${counter} AuthzInvitationsTokenIdByEmail rows...`
    );

    const queryResultOnSource = result;
    result = await targetClient.execute(
        query,
        [store.allInvitationEmails],
        clientOptions
    );
    util.compareBothTenants(
        queryResultOnSource.rows.length,
        result.rows.length
    );
};

const copyAuthzInvitationsEmailByToken = async function (
	sourceClient,
	targetClient
) {
	if (_.isEmpty(store.allInvitationTokens)) {
        logger.info(
            chalk.cyan(
                `✗  Skipped fetching AuthzInvitationsEmailByToken rows...\n`
            )
        );
        return [];
	}

	const query = `SELECT * FROM "AuthzInvitationsEmailByToken" WHERE "token" IN ? LIMIT ${
		clientOptions.fetchSize
	}`;
	const insertQuery = `INSERT INTO "AuthzInvitationsEmailByToken" ("token", email) VALUES (?, ?)`;
	let counter = 0;

	let result = await sourceClient.execute(
        query,
        [store.allInvitationTokens],
        clientOptions
    );

	async function insertAll(targetClient, rows) {
		for (let i = 0; i < rows.length; i++) {
			const row = rows[i];
			counter++;

			await targetClient.execute(
                insertQuery,
                [row.token, row.email],
                clientOptions
            );
		}
	}

    logger.info(
        `${chalk.green(`✓`)}  Fetched ${
        	result.rows.length
        } AuthzInvitationsEmailByToken rows...`
    );
    if (_.isEmpty(result.rows)) {
    	return;
    }
    await insertAll(targetClient, result.rows);
    logger.info(
        `${chalk.green(
            `✓`
        )}  Inserted ${counter} AuthzInvitationsEmailByToken rows...`
    );

    const queryResultOnSource = result;
    result = await targetClient.execute(
        query,
        [store.allInvitationTokens],
        clientOptions
    );
    util.compareBothTenants(
        queryResultOnSource.rows.length,
        result.rows.length
    );
};

module.exports = {
	copyAuthzInvitations,
	copyAuthzInvitationsEmailByToken,
	copyAuthzInvitationsResourceIdByEmail,
	copyAuthzInvitationsTokenByEmail
};
