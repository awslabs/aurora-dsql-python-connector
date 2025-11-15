# Aurora DSQL Connector for Python

[![GitHub](https://img.shields.io/badge/github-awslabs/aurora--dsql--python--connector-blue?logo=github)](https://github.com/awslabs/aurora-dsql-python-connector)
[![License](https://img.shields.io/badge/license-Apache--2.0-brightgreen)](https://github.com/awslabs/aurora-dsql-python-connector/blob/main/LICENSE)
[![PyPI - Version](https://img.shields.io/pypi/v/aurora-dsql-python-connector)](https://pypi.org/project/aurora-dsql-python-connector)
[![Discord chat](https://img.shields.io/discord/1435027294837276802.svg?logo=discord)](https://discord.com/invite/nEF6ksFWru)

The Aurora DSQL Connector for Python integrates IAM Authentication for connecting Python applications to Amazon Aurora DSQL clusters.
Internally, it utilizes [psycopg](https://github.com/psycopg/psycopg), [psycopg2](https://github.com/psycopg/psycopg2), and [asyncpg](https://github.com/MagicStack/asyncpg) client libraries.

The Aurora DSQL Connector for Python is designed as an authentication plugin that extends the functionality of the psycopg, psycopg2, and asyncpg
client libraries to enable applications to authenticate with Amazon Aurora DSQL using IAM credentials. The connector 
does not connect directly to the database but provides seamless IAM authentication on top of the underlying client libraries.

## About the Connector

Amazon Aurora DSQL is a distributed SQL database service that provides high availability and scalability for 
PostgreSQL-compatible applications. Aurora DSQL requires IAM-based authentication with time-limited tokens that 
existing Python libraries do not natively support.

The idea behind the Aurora DSQL Connector for Python is to add an authentication layer on top of the psycopg, psycopg2, and asyncpg
client libraries that handles IAM token generation, allowing users to connect to Aurora DSQL without changing their existing workflows.

### Features

- **Automatic IAM Authentication** - Handles DSQL token generation
- **Built on psycopg and psycopg2** - Leverages the psycopg and psycopg2 client libraries
- **Region Auto-Discovery** - Extracts AWS region from DSQL cluster hostname
- **Custom Credentials** - Support for custom AWS credential providers

## Quick start guide

### Requirements

- Python 3.10 or higher
- AWS credentials configured (via AWS CLI, environment variables, or IAM roles)
- Access to an Aurora DSQL cluster


### Installation

```bash
pip install aurora-dsql-python-connector
```

#### Install psycopg or psycopg2 or asyncpg separately

The Aurora DSQL Connector for Python installer does not install the underlying libraries.
They need to be installed separately, e.g.:


```bash
# The command below installs psycopg and psycopg pool
pip install "psycopg[binary,pool]"

# OR

# The command below installs psycopg2
pip install psycopg2-binary

# OR

# The command below installs asyncpg
pip install asyncpg
```

**Note:**

Only the library that is needed must be installed.
Therefore, if the client is going to use psycopg, then only psycopg needs to be installed.
If the client is going to use psycopg2, then only psycopg2 needs to be installed.
If the client is going to use asyncpg, then only asyncpg needs to be installed.

If the client needs more than one, then all the needed libraries need to be installed.

### Basic Usage 

#### psycopg

```python
    # Use this import for psycopg
    import aurora_dsql_psycopg as dsql

    # Use this import for psycopg2
    import aurora_dsql_psycopg2 as dsql

    config = {
        'host': "your-cluster.dsql.us-east-1.on.aws",
        'region': "us-east-1",
        'user': "admin",
    }
        
    conn = dsql.connect(**config)
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        result = cur.fetchone()
        print(result)
```

#### asyncpg

```python
    import asyncio 
    import aurora_dsql_asyncpg as dsql 

    config = {
        'host': "your-cluster.dsql.us-east-1.on.aws",
        'region': "us-east-1",
        'user': "admin",
    }

    conn = await dsql.connect(**config)
    result = await conn.fetchrow("SELECT 1")
    await conn.close()
    print(result)
```

#### Using just host

##### psycopg

```python
    # Use this import for psycopg
    import aurora_dsql_psycopg as dsql

    # Use this import for psycopg2
    import aurora_dsql_psycopg2 as dsql

    conn = dsql.connect("your-cluster.dsql.us-east-1.on.aws")
```

##### asyncpg

```python
    import asyncio 
    import aurora_dsql_asyncpg as dsql 

    conn = await dsql.connect("your-cluster.dsql.us-east-1.on.aws")
```


#### Using just cluster ID

##### psycopg

```python
    # Use this import for psycopg
    import aurora_dsql_psycopg as dsql

    # Use this import for psycopg2
    import aurora_dsql_psycopg2 as dsql

    conn = dsql.connect("your-cluster")
```

##### asyncpg

```python
    import asyncio 
    import aurora_dsql_asyncpg as dsql 

    conn = await dsql.connect("your-cluster")
```

**Note:** 

In the above scenario, the region is used that was set previously on the machine, e.g.:

```bash
aws configure set region us-east-1
```

If the region has not been set, or the given cluster ID is in a different region, the connection will fail.
To make it work, provide region as a parameter as in the example below:

##### psycopg

```python
    # Use this import for psycopg
    import aurora_dsql_psycopg as dsql

    # Use this import for psycopg2
    import aurora_dsql_psycopg2 as dsql

    config = {
            "region": "us-east-1",
    }

    conn = dsql.connect("your-cluster", **config)
```

##### asyncpg

```python
    import asyncio 
    import aurora_dsql_asyncpg as dsql 

    config = {
            "region": "us-east-1",
    }

    conn = await dsql.connect("your-cluster", **config)
```

### Connection String

#### psycopg

```python
    # Use this import for psycopg
    import aurora_dsql_psycopg as dsql

    # Use this import for psycopg2
    import aurora_dsql_psycopg2 as dsql

    conn = dsql.connect("postgresql://your-cluster.dsql.us-east-1.on.aws/postgres?user=admin&token_duration_secs=15")
```

#### asyncpg

```python
    import asyncio 
    import aurora_dsql_asyncpg as dsql 

    conn = await dsql.connect("postgresql://your-cluster.dsql.us-east-1.on.aws/postgres?user=admin&token_duration_secs=15")
```

### Advanced Configuration

#### psycopg

```python
    # Use this import for psycopg
    import aurora_dsql_psycopg as dsql

    # Use this import for psycopg2
    import aurora_dsql_psycopg2 as dsql

    config = {
        'host': "your-cluster.dsql.us-east-1.on.aws",
        'region': "us-east-1",
        'user': "admin",
        "profile": "default",
        "token_duration_secs": "15",
    }
        
    conn = dsql.connect(**config)
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        result = cur.fetchone()
        print(result)
```

#### asyncpg

```python
    import asyncio 
    import aurora_dsql_asyncpg as dsql 

    config = {
        'host': "your-cluster.dsql.us-east-1.on.aws",
        'region': "us-east-1",
        'user': "admin",
        "profile": "default",
        "token_duration_secs": "15",
    }

    conn = await dsql.connect(**config)
    result = await conn.fetchrow("SELECT 1")
    await conn.close()
    print(result)
```


## Configuration Options

| Option                        | Type                     | Required | Description                                                   |
|-------------------------------|--------------------------|----------|---------------------------------------------------------------|
| `host`                        | `string`                 | Yes      | DSQL cluster hostname or cluster ID                           |
| `user`                        | `string`                 | No       | DSQL username. Default: admin                                 |
| `dbname`                      | `string`                 | No       | Database name.  Default: postgres                             |
| `region`                      | `string`                 | No       | AWS region (auto-detected from hostname if not provided)      |
| `port`                        | `int`                    | No       | Default to 5432                                               |
| `custom_credentials_provider` | `CredentialProvider`     | No       | Custom AWS credentials provider                               |
| `profile`                     | `string`                 | No       | The IAM profile name. Default: default.                       |
| `token_duration_secs`         | `int`                    | No       | Token expiration time in seconds                              |


All standard connection options of the underlying psycopg and psycopg2 libraries are also supported.

## Authentication

The connector automatically handles DSQL authentication by generating tokens using the DSQL client token generator. If the
AWS region is not provided, it will be automatically parsed from the hostname provided.

### Admin vs Regular Users

- Users named `"admin"` automatically use admin authentication tokens
- All other users use non-admin authentication tokens
- Tokens are generated dynamically for each connection

## Examples

For full example code, refer to the examples directory in this repository as indicated in the sections below.

### psycopg


| Description                                                                     | Examples                                                                                                                                                                                   |
|---------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Using the Aurora DSQL Connector for Python for basic connections                | [Basic Connection Example](https://github.com/awslabs/aurora-dsql-python-connector/tree/main/examples/psycopg/src/example.py)                                                              |
| Using the Aurora DSQL Connector for Python for basic asynchronous connections   | [Basic Asynchronous Connection Example](https://github.com/awslabs/aurora-dsql-python-connector/tree/main/examples/psycopg/src/example_async.py)                                           |
| Using the Aurora DSQL Connector for Python with connection pool                 | [Basic Connection Example With Connection Pool](https://github.com/awslabs/aurora-dsql-python-connector/tree/main/examples/psycopg/src/example_with_connection_pool.py)                    |
|                                                                                 | [Concurrent Connections Example With Connection Pool](https://github.com/awslabs/aurora-dsql-python-connector/tree/main/examples/psycopg/src/example_with_connection_pool_concurrent.py)   |
| Using the Aurora DSQL Connector for Python with asynchronous connection pool    | [Basic Connection Example With Asynchronous Connection Pool](https://github.com/awslabs/aurora-dsql-python-connector/tree/main/examples/psycopg/src/example_with_connection_pool_async.py) |


### psycopg2 


| Description                                                                     | Examples                                                                                                                                                                                   |
|---------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Using the Aurora DSQL Connector for Python for basic connections                | [Basic Connection Example](https://github.com/awslabs/aurora-dsql-python-connector/tree/main/examples/psycopg2/src/example.py)                                                             |
| Using the Aurora DSQL Connector for Python with connection pool                 | [Basic Connection Example With Connection Pool](https://github.com/awslabs/aurora-dsql-python-connector/tree/main/examples/psycopg2/src/example_with_connection_pool.py)                   |
|                                                                                 | [Concurrent Connections Example With Connection Pool](https://github.com/awslabs/aurora-dsql-python-connector/tree/main/examples/psycopg2/src/example_with_connection_pool_concurrent.py)  |


### asyncpg

| Description                                                                     | Examples                                                                                                                                                                                   |
|---------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Using the Aurora DSQL Connector for Python for basic connections                | [Basic Connection Example](https://github.com/awslabs/aurora-dsql-python-connector/tree/main/examples/asyncpg/src/example.py)                                                              |
| Using the Aurora DSQL Connector for Python with connection pool                 | [Basic Connection Example With Connection Pool](https://github.com/awslabs/aurora-dsql-python-connector/tree/main/examples/asyncpg/src/example_with_connection_pool.pyv)                   |
|                                                                                 | [Concurrent Connections Example With Connection Pool](https://github.com/awslabs/aurora-dsql-python-connector/tree/main/examples/asyncpg/src/example_with_connection_pool_concurrent.py)   |



## Development

```bash
# Install dependencies
pip install -e ".[psycopg,psycopg2,asyncpg,dev]"

# Run unit tests
python -m pytest tests/unit/

# Set a cluster for use in integration tests
export CLUSTER_ENDPOINT=your-cluster.dsql.us-east-1.on.aws

# Run integration tests
python -m pytest tests/integration/
```

## License
This software is released under the Apache 2.0 license.

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
