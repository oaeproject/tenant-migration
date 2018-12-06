const chalk = require('chalk');
const logger = require('../logger');
const _ = require('underscore');
const { fetchGroupsFromTenant, fetchPrincipalDetails } = require('./dao');

const doGroupMaintenance = async function(source, destination) {
  let memberships = [];

  let transferredGroups = await fetchGroupsFromTenant(source);
  for await (const eachGroup of transferredGroups) {
    let group = await fetchPrincipalDetails(source, eachGroup.resourceId);
    let member = await fetchPrincipalDetails(source, eachGroup.memberId);

    if (group) {
      memberships.push({
        group: group.displayName,
        member: `${member.displayName} from ${member.tenantAlias}`
      });
    } else {
      logger.warn(`${chalk.red('Group ' + eachGroup.resourceId + ' is undefined')}`);
    }
  }

  let membershipsByGroup = _.groupBy(memberships, eachMembership => {
    return eachMembership.group;
  });

  _.each(_.keys(membershipsByGroup), eachGroup => {
    let memberList = membershipsByGroup[eachGroup].map(eachMember => eachMember.member);
    membershipsByGroup[eachGroup] = memberList;

    // print stuff for easy export
    console.log(chalk.red(eachGroup));
    // console.log(`  ${chalk.green(newMemberList.join(',\n  '))}`);
    console.log(`  ${chalk.green(membershipsByGroup[eachGroup].join(',\n  '))}`);
    console.log();
  });
};

module.exports = {
  doGroupMaintenance
};
