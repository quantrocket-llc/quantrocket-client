#!/bin/bash -e

# PYPI_USERNAME - (Requried) Username for the publisher's account on PyPI
# PYPI_PASSWORD - (Required, Secret) Password for the publisher's account on PyPI

cat <<EOF >> ~/.pypirc
[distutils]
index-servers=pypi

[pypi]
repository=https://pypi.python.org/pypi
username=$PYPI_USERNAME
password=$PYPI_PASSWORD
EOF

# Deploy to pip
python setup.py register
python setup.py sdist bdist_wheel upload

# Rebuild quantrocker/cli Docker image with latest client
curl https://registry.hub.docker.com/u/quantrocket/cli/trigger/5f686645-bdd6-46f3-b90a-76d3a15a6526/