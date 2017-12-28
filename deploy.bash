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

# Rebuild quantrocker/jupyter Docker image with latest client
curl -X POST https://registry.hub.docker.com/u/quantrocket/jupyter/trigger/41f6af9a-16bd-47c7-a088-71076407a7cc/
