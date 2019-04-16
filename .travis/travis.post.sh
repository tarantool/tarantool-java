#!/bin/sh

set -exu # Strict shell (w/o -o pipefail)

if [ "${TRAVIS_JDK_VERSION}" = openjdk11 ]; then
    mvn coveralls:report -DrepoToken=${COVERALLS_TOKEN}
fi
