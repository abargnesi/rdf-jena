#!/usr/bin/env bash

mvn -q -o package -DskipTests

cp target/rdf-jena-1.0.0.jar lib/rdf/jena/jars/

#export JAVA_OPTS="-ea -Xdebug -Xrunjdwp:transport=dt_socket,address=1044,server=y,suspend=y"

JRUBY_HOME=$(realpath jruby) \
    PATH="$JRUBY_HOME/bin:$PATH" \
    GEM_HOME=.gem \
    GEM_PATH=.gem \
    .gem/bin/pry -I "./lib" -r "rdf/jena"
