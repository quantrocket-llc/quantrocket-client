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

# Rebuild jupyter, countdown, satellite, moonshot Docker images with latest
# client (and images that might run user code)
curl -X POST https://registry.hub.docker.com/u/quantrocket/jupyter/trigger/41f6af9a-16bd-47c7-a088-71076407a7cc/
curl -X POST https://registry.hub.docker.com/u/quantrocket/countdown/trigger/b83e185e-4702-4f41-8d4b-b7b83a24899e/
curl -X POST https://registry.hub.docker.com/u/quantrocket/satellite/trigger/16c37457-72e1-4415-b0c4-c5185684b350/
curl -X POST https://registry.hub.docker.com/u/quantrocket/moonshot/trigger/4a8ae8f2-f85a-4f2f-8b5d-054260e96287/
