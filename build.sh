#!/usr/bin/env bash
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

pushd "$DIR" > /dev/null

mvn -q -o package -DskipTests
cp target/rdf-jena-0.4.0.beta.jar lib/rdf/jena/jars/

JRUBY_OPTS="--dev" gem build rdf-jena.gemspec
JRUBY_OPTS="--dev" gem install rdf-jena-0.4.0.beta-java.gem

popd > /dev/null
