#!/usr/bin/env bash
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

pushd "$DIR" > /dev/null

mvn -q -o package -DskipTests
cp target/rdf-jena-0.3.0.jar lib/rdf/jena/jars/

popd > /dev/null
