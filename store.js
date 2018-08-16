/*!
 * Copyright 2018 Apereo Foundation (AF) Licensed under the
 * Educational Community License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 *     http=//opensource.org/licenses/ECL-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

function Store() {
    Store.init = function() {
        Store.tenantPrincipals = [];
        Store.tenantUsers = [];
        Store.tenantGroups = [];
        Store.folderGroups = [];
        Store.allInvitationEmails = [];
        Store.allInvitationTokens = [];
        Store.allOauthClientsIds = [];
        Store.folderGroupIdsFromThisTenancyAlone = [];
        Store.threadKeysFromThisTenancyAlone = [];
        Store.discussionsFromThisTenancyAlone = [];
        Store.allRevisionIds = [];
        Store.allContentIds = [];
        Store.allResourceIds = [];
        Store.allLoginIds = [];
        Store.allTenantMessages = [];
    };

    Store.getAttribute = function(attribute) {
        return Store[attribute].slice(0);
    };

    Store.setAttribute = function(attribute, value) {
        Store[attribute] = value;
    };
}

module.exports = {
    Store
};
