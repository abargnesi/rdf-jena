#!/usr/bin/env bash
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

. build.sh

pushd "$DIR" > /dev/null

#export JAVA_OPTS="-ea -Xdebug -Xrunjdwp:transport=dt_socket,address=1044,server=y,suspend=y"
export JRUBY_OPTS="-J-Xmx8g"
JRUBY_HOME=$(realpath jruby) \
    PATH="$JRUBY_HOME/bin:$PATH" \
    GEM_HOME=.gem \
    GEM_PATH=.gem \
    .gem/bin/pry -I "./lib" -r "rdf/jena"

popd > /dev/null
