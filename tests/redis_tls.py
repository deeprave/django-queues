"""Generate short-lived TLS material for Redis integration fixtures."""

from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

_CA_CONFIG = """\
[req]
distinguished_name = req_dn
x509_extensions = v3_ca
prompt = no
[req_dn]
CN = django-queues-test-ca
[v3_ca]
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer
basicConstraints = critical, CA:true
keyUsage = critical, keyCertSign, cRLSign
"""

_SAN_CONFIG = """\
[req]
distinguished_name = req_dn
x509_extensions = v3_req
prompt = no
[req_dn]
CN = localhost
[v3_req]
subjectAltName = @alt_names
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
[alt_names]
DNS.1 = localhost
DNS.2 = host.docker.internal
IP.1 = 127.0.0.1
IP.2 = ::1
IP.3 = 172.17.0.1
"""


@dataclass(frozen=True, slots=True)
class RedisTlsMaterial:
    """Paths to a generated test CA, server certificate, and untrusted CA."""

    directory: Path
    ca_cert: Path
    server_cert: Path
    server_key: Path
    untrusted_ca_cert: Path


def openssl_available() -> bool:
    return shutil.which("openssl") is not None


def generate_redis_tls_material(directory: Path) -> RedisTlsMaterial:
    """Create a CA, server cert/key, and a second CA that did not issue the server cert."""
    directory.mkdir(parents=True, exist_ok=True)
    directory.chmod(0o755)
    ca_config = directory / "ca.cnf"
    san_config = directory / "san.cnf"
    ca_config.write_text(_CA_CONFIG)
    san_config.write_text(_SAN_CONFIG)
    ca_key = directory / "ca.key"
    ca_cert = directory / "ca.crt"
    server_key = directory / "server.key"
    server_csr = directory / "server.csr"
    server_cert = directory / "server.crt"
    untrusted_key = directory / "untrusted.key"
    untrusted_ca_cert = directory / "untrusted.crt"

    _openssl(
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        str(ca_key),
        "-out",
        str(ca_cert),
        "-days",
        "1",
        "-subj",
        "/CN=django-queues-test-ca",
        "-extensions",
        "v3_ca",
        "-config",
        str(ca_config),
    )
    _openssl(
        "req",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        str(server_key),
        "-out",
        str(server_csr),
        "-subj",
        "/CN=localhost",
    )
    _openssl(
        "x509",
        "-req",
        "-in",
        str(server_csr),
        "-CA",
        str(ca_cert),
        "-CAkey",
        str(ca_key),
        "-CAcreateserial",
        "-out",
        str(server_cert),
        "-days",
        "1",
        "-extfile",
        str(san_config),
        "-extensions",
        "v3_req",
    )
    _openssl(
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        str(untrusted_key),
        "-out",
        str(untrusted_ca_cert),
        "-days",
        "1",
        "-subj",
        "/CN=django-queues-untrusted-ca",
        "-extensions",
        "v3_ca",
        "-config",
        str(ca_config),
    )
    for path in (ca_cert, server_cert, server_key, untrusted_ca_cert):
        path.chmod(0o644)
    return RedisTlsMaterial(
        directory=directory,
        ca_cert=ca_cert,
        server_cert=server_cert,
        server_key=server_key,
        untrusted_ca_cert=untrusted_ca_cert,
    )


def _openssl(*args: str) -> None:
    subprocess.run(
        ["openssl", *args],
        check=True,
        capture_output=True,
        text=True,
    )
