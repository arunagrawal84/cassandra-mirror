#!/bin/bash
set -o errexit
curl -LO https://github.com/rlmcpherson/s3gof3r/releases/download/v0.5.0/gof3r_0.5.0_linux_amd64.tar.gz
echo d88f199d1580d8c8cac26ba39e15dc6e2126d20e56c3894bd37a226e8b3e665c gof3r_0.5.0_linux_amd64.tar.gz | sha256sum -c
tar --strip-components 1 -C cassandra_mirror -xvf gof3r_0.5.0_linux_amd64.tar.gz
