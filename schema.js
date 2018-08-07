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

module.exports = [
    {
        query: `CREATE TABLE IF NOT EXISTS "Principals" ("principalId" text PRIMARY KEY, "tenantAlias" text, "displayName" text, "description" text, "email" text, "emailPreference" text, "visibility" text, "joinable" text, "lastModified" text, "locale" text, "publicAlias" text, "largePictureUri" text, "mediumPictureUri" text, "smallPictureUri" text, "admin:global" text, "admin:tenant" text, "notificationsUnread" text, "notificationsLastRead" text, "acceptedTC" text, "createdBy" text, "created" timestamp, "deleted" timestamp)`
    },
    {
        query: `CREATE TABLE IF NOT EXISTS "PrincipalsByEmail" ("email" text, "principalId" text, PRIMARY KEY ("email", "principalId"))`
    },
    {
        query: `CREATE TABLE IF NOT EXISTS "Tenant" ("alias" text PRIMARY KEY, "displayName" text, "host" text, "emailDomains" text, "countryCode" text, "active" boolean)`
    },
    {
        query: `CREATE TABLE IF NOT EXISTS "Folders" ("id" text PRIMARY KEY, "tenantAlias" text, "groupId" text, "displayName" text, "visibility" text, "description" text, "createdBy" text, "created" bigint, "lastModified" bigint, "previews" text)`
    },
    {
        query: `CREATE TABLE IF NOT EXISTS "FoldersGroupId" ("groupId" text PRIMARY KEY, "folderId" text)`
    },
    {
        query: `CREATE TABLE IF NOT EXISTS "AuthzMembers" ("resourceId" text, "memberId" text, "role" text, PRIMARY KEY ("resourceId", "memberId")) WITH COMPACT STORAGE`
    },
    {
        query: `CREATE TABLE IF NOT EXISTS "AuthzRoles" ("principalId" text, "resourceId" text, "role" text, PRIMARY KEY ("principalId", "resourceId")) WITH COMPACT STORAGE`
    },
    {
        query: `CREATE TABLE IF NOT EXISTS "Content" ("contentId" text PRIMARY KEY, "tenantAlias" text, "visibility" text, "displayName" text, "description" text, "resourceSubType" text, "createdBy" text, "created" text, "lastModified" text, "latestRevisionId" text, "uri" text, "previews" text, "status" text, "largeUri" text, "mediumUri" text, "smallUri" text, "thumbnailUri" text, "wideUri" text, "etherpadGroupId" text, "etherpadPadId" text, "filename" text, "link" text, "mime" text, "size" text)`
    },
    {
        query:
            'CREATE TABLE IF NOT EXISTS "RevisionByContent" ("contentId" text, "created" text, "revisionId" text, PRIMARY KEY ("contentId", "created")) WITH COMPACT STORAGE'
    },
    {
        query: `CREATE TABLE IF NOT EXISTS "Revisions" ("revisionId" text PRIMARY KEY, "contentId" text, "created" text, "createdBy" text, "filename" text, "mime" text, "size" text, "uri" text, "previewsId" text, "previews" text, "status" text, "largeUri" text, "mediumUri" text, "smallUri" text, "thumbnailUri" text, "wideUri" text, "etherpadHtml" text)`
    },
    {
        query:
            'CREATE TABLE IF NOT EXISTS "Discussions" ("id" text PRIMARY KEY, "tenantAlias" text, "displayName" text, "visibility" text, "description" text, "createdBy" text, "created" text, "lastModified" text)'
    },
    {
        query:
            'CREATE TABLE IF NOT EXISTS "Messages" ("id" text PRIMARY KEY, "threadKey" text, "createdBy" text, "body" text, "deleted" text)'
    },
    {
        query:
            'CREATE TABLE IF NOT EXISTS "MessageBoxMessages" ("messageBoxId" text, "threadKey" text, "value" text, PRIMARY KEY ("messageBoxId", "threadKey")) WITH COMPACT STORAGE'
    },
    {
        query:
            'CREATE TABLE IF NOT EXISTS "MessageBoxMessagesDeleted" ("messageBoxId" text, "createdTimestamp" text, "value" text, PRIMARY KEY ("messageBoxId", "createdTimestamp")) WITH COMPACT STORAGE'
    },
    {
        query:
            'CREATE TABLE IF NOT EXISTS "MessageBoxRecentContributions" ("messageBoxId" text, "contributorId" text, "value" text, PRIMARY KEY ("messageBoxId", "contributorId")) WITH COMPACT STORAGE'
    },
    {
        query:
            'CREATE TABLE IF NOT EXISTS "FollowingUsersFollowers" ("userId" text, "followerId" text, "value" text, PRIMARY KEY ("userId", "followerId")) WITH COMPACT STORAGE'
    },
    {
        query:
            'CREATE TABLE IF NOT EXISTS "FollowingUsersFollowing" ("userId" text, "followingId" text, "value" text, PRIMARY KEY ("userId", "followingId")) WITH COMPACT STORAGE'
    },
    {
        query:
            'CREATE TABLE IF NOT EXISTS "UsersGroupVisits" ("userId" text, "groupId" text, "latestVisit" text, PRIMARY KEY ("userId", "groupId"))'
    },
    {
        query:
            'CREATE TABLE IF NOT EXISTS "AuthenticationLoginId" ("loginId" text PRIMARY KEY, "userId" text, "password" text, "secret" text)'
    },
    {
        query:
            'CREATE TABLE IF NOT EXISTS "AuthenticationUserLoginId" ("userId" text, "loginId" text, "value" text, PRIMARY KEY ("userId", "loginId")) WITH COMPACT STORAGE'
    },
    {
        query:
            'CREATE TABLE IF NOT EXISTS "OAuthAccessToken" ("token" text PRIMARY KEY, "userId" text, "clientId" text)'
    },
    {
        query:
            'CREATE TABLE IF NOT EXISTS "OAuthAccessTokenByUser" ("userId" text, "clientId" text, "token" text, PRIMARY KEY ("userId", "clientId")) WITH COMPACT STORAGE'
    },

    {
        query:
            'CREATE TABLE IF NOT EXISTS "Config" ("tenantAlias" text, "configKey" text, "value" text, PRIMARY KEY ("tenantAlias", "configKey")) WITH COMPACT STORAGE'
    },

    {
        query:
            'CREATE TABLE IF NOT EXISTS "AuthzInvitations" ("resourceId" text, "email" text, "inviterUserId" text, "role" text, PRIMARY KEY ("resourceId", "email"))'
    },

    {
        query:
            'CREATE TABLE IF NOT EXISTS "AuthzInvitationsResourceIdByEmail" ("email" text, "resourceId" text, PRIMARY KEY ("email", "resourceId"))'
    },

    {
        query:
            'CREATE TABLE IF NOT EXISTS "AuthzInvitationsTokenByEmail" ("email" text PRIMARY KEY, "token" text)'
    },

    {
        query:
            'CREATE TABLE IF NOT EXISTS "AuthzInvitationsEmailByToken" ("token" text PRIMARY KEY, "email" text)'
    },
    {
        query: `CREATE INDEX IF NOT EXISTS ON "Principals" ("tenantAlias")`
    }
];
