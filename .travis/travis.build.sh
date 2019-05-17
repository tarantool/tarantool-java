#!/bin/sh

set -exu # Strict shell (w/o -o pipefail)

if [ "${TRAVIS_JDK_VERSION}" = "openjdk11" ] && [ "${TNT_VERSION}" = "2.2" ]; then
    mvn verify jacoco:report
else
    mvn verify
fi

head -n -0 testroot/*.log
