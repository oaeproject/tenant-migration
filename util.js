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
const logger = require('./logger');

const compareBothTenants = function (queryResultOnSource, queryResultOnTarget) {
	let logMessage = '';
	let logMark = '';

	if (queryResultOnSource === queryResultOnTarget) {
		logMark = '✓';
		logMessage = `Numbers of rows fetched/inserted match`;
        logger.info(chalk.green(`${logMark}  ${logMessage}...\n`));
	} else {
		logMark = '✗';
		logMessage = `Number of rows fetched/inserted don't match`;
        logger.info(chalk.red(`${logMark}  ${logMessage}...\n`));
	}
};

module.exports = {
	compareBothTenants
};
