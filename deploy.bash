#!/bin/bash -e

# PYPI_USERNAME - (Requried) Username for the publisher's account on PyPI
# PYPI_PASSWORD - (Required, Secret) Password for the publisher's account on PyPI

cat <<'EOF' >> .pypirc
[pypi]
repository=https://pypi.python.org/pypi
username=$PYPI_USERNAME
password=$PYPI_PASSWORD
EOF
python setup.py sdist bdist_wheel upload
