# Tenant migration
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Foaeproject%2Ftenant-migration.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Foaeproject%2Ftenant-migration?ref=badge_shield)


This is a repo containing a program used to migrate a tenant's data from one OAE instance to another.

This program consists of two parts:

- Firstly, it copies database rows from one database to another (both must be accessible)
- Secondly, it transfer the files and assets of the source server to the local machine, and then from the local machine to the destination server. This mechanism uses rsync.

# Prerequisites

- redis
- rsync
- node > 8.9.4
- npm

# Installation

First we need to install dependencies:

```
git clone https://github.com/oaeproject/tenant-migration.git
cd tenant-migration
npm install
```

Make sure both node, redis and rsync are working locally:

```
redis-cli --version
rsync --version
node -v
```

All should return a valid version.

# Usage

## Configuration

There are two configuration files in place: `source.json` and `destination.json`. Both have the same format and represent the source and destination OAE instances, respectively. Here's the format of the configuration:

```
{
    "sourceDatabase": {
        "keyspace": "oae",
        "host": "localhost",
        "timeout": 6000,
        "tenantAlias": "apereo",
        "strategyClass": "SimpleStrategy",
        "replication": 3
    },
    "sourceFileHost": {
        "host": "monitor.aws.oaeproject.org",
        "user": "root",
        "path": "/shared/"
    }
}
```

Description of each of the fields:

- `database.keyspace` is the keyspace we're copying from / to
- `database.host` is the hostname or IP address or the cassandra server on each end (origin/destination)
- `database.timeout` how much we are willing to wait until a timeout error is thrown while connecting
- `database.tenantAlias` is the `alias` of the tenant we're transferring from one instance to the other

- `files.host` is the hostname of where the files are stored on each end (origin/destination)
- `files.user` is the user of the ssh connection that is established in order to run rsync
- `files.path` is the root path where the files are stored within OAE

## Execution

Once configuration is in place, one can run the migration as follows:

```
node index.js
```

The application is quite verbose so if there's an error, it will be displayed.

If you're testing this on an empty cassandra keyspace, then it is possible that the the first execution fails with this error:

```
error: ResponseError: unconfigured columnfamily Discussions # <- or maybe some other column family
```

This happens because the script tries to execute before Cassandra has had the time to create the tables. If this happens, just run it again (the tables will have been created by then).

## Reference links

The complex part is the one regarding etherpad data transfer, mostly because it's stored in a key-value format in Cassandra. Here are some links regarding how etherpad works:

- [HTTP API](https://github.com/ether/etherpad-lite/wiki/HTTP-API)
- [Etherpad docs](http://etherpad.org/doc/v1.5.6/#index_about_this_documentation)


## License
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Foaeproject%2Ftenant-migration.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Foaeproject%2Ftenant-migration?ref=badge_large)