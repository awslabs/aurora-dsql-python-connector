# Aurora DSQL with Psycopg2

## Overview

This code example demonstrates how to use Psycopg (version 2) with Amazon Aurora DSQL. The example shows you how to
connect to an Aurora DSQL cluster and perform basic database operations.

Aurora DSQL is a distributed SQL database service that provides high availability and scalability for
your PostgreSQL-compatible applications. Psycopg2 is a popular PostgreSQL adapter for Python that allows
you to interact with PostgreSQL databases using Python code.

## About the code example

The example demonstrates a flexible connection approach that works for both admin and non-admin users:

* When connecting as an **admin user**, the example uses the `public` schema and generates an admin authentication
  token.
* When connecting as a **non-admin user**, the example uses a custom `myschema` schema and generates a standard
  authentication token.

The code automatically detects the user type and adjusts its behavior accordingly.

## ⚠️ Important

* Running this code might result in charges to your AWS account.
* We recommend that you grant your code least privilege. At most, grant only the
  minimum permissions required to perform the task. For more information, see
  [Grant least privilege](https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html#grant-least-privilege).
* This code is not tested in every AWS Region. For more information, see
  [AWS Regional Services](https://aws.amazon.com/about-aws/global-infrastructure/regional-product-services).

## TLS connection configuration

This example uses direct TLS connections where supported, and verifies the server certificate is trusted. Verified SSL
connections should be used where possible to ensure data security during transmission.

* Driver versions following the release of PostgreSQL 17 support direct TLS connections, bypassing the traditional
  PostgreSQL connection preamble
* Direct TLS connections provide improved connection performance and enhanced security
* Not all PostgreSQL drivers support direct TLS connections yet, or only in recent versions following PostgreSQL 17
* Ensure your installed driver version supports direct TLS negotiation, or use a version that is at least as recent as
  the one used in this sample
* If your driver doesn't support direct TLS connections, you may need to use the traditional preamble connection instead

## Run the example

### Prerequisites

* You must have an AWS account, and have your default credentials and AWS Region
  configured as described in the
  [Globally configuring AWS SDKs and tools](https://docs.aws.amazon.com/credref/latest/refdocs/creds-config-files.html)
  guide.
* [Python 3.10.0](https://www.python.org/) or later.
* You must have an Aurora DSQL cluster. For information about creating an Aurora DSQL cluster, see the
  [Getting started with Aurora DSQL](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/getting-started.html)
  guide.
* If connecting as a non-admin user, ensure the user is linked to an IAM role and is granted access to the `myschema`
  schema. See the
  [Using database roles with IAM roles](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/using-database-and-iam-roles.html)
  guide.

### Download the Amazon root certificate from the official trust store

Download the Amazon root certificate from the official trust store:

```
wget https://www.amazontrust.com/repository/AmazonRootCA1.pem -O root.pem
```

### Set up environment for examples

1. Create and activate a Python virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate  # Linux, macOS
# or
.venv\Scripts\activate     # Windows
```

2. Install the required packages for running the examples:

```bash
pip install -e .

# Install optional dependencies for tests
pip install -e ".[test]"
```

### Run the code

The example demonstrates the following operations:

- Opening a connection to an Aurora DSQL cluster
- Creating a table
- Inserting and querying data

The example is designed to work with both admin and non-admin users:

- When run as an admin user, it uses the `public` schema
- When run as a non-admin user, it uses the `myschema` schema

**Note:** running the example will use actual resources in your AWS account and may incur charges.

Set environment variables for your cluster details:

```bash
# e.g. "admin"
export CLUSTER_USER="<your user>"

# e.g. "foo0bar1baz2quux3quuux4.dsql.us-east-1.on.aws"
export CLUSTER_ENDPOINT="<your endpoint>"

# e.g. "us-east-1"
export REGION="<your region>"
```

Run the example:

```bash
# Run example directly
python src/example.py

# Run example using pytest
pytest ./test/test_example.py

# Run all using pytest
pytest ./test
```

The example contains comments explaining the code and the operations being performed.

## Additional resources

* [Amazon Aurora DSQL Documentation](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/what-is-aurora-dsql.html)
* [Psycopg2 Documentation](https://www.psycopg.org/docs/)
* [AWS SDK for Python (Boto3) Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)

---

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

SPDX-License-Identifier: MIT-0
