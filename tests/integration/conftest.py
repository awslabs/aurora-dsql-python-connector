"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import tempfile
import urllib.request

import pytest


@pytest.fixture(scope="session")
def ssl_cert_path():
    """Download Amazon root certificate if not present."""
    cert_path = os.environ.get("SSL_CERT_PATH")
    if cert_path:
        return cert_path

    cert_path = os.path.join(tempfile.gettempdir(), "AmazonRootCA1.pem")
    if not os.path.exists(cert_path):
        urllib.request.urlretrieve(
            "https://www.amazontrust.com/repository/AmazonRootCA1.pem",
            cert_path,
        )
    return cert_path
