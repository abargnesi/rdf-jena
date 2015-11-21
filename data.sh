#!/usr/bin/env bash

export JENA_HOME=$(realpath jena/)

TMPDIR=/tmp jena/bin/tdbloader2 -l data/ "data.nt"
