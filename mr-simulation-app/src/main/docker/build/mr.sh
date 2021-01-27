#!/usr/bin/env sh
java -Dnashorn.args=--no-deprecation-warning -jar /usr/lib/mr-simulation/mr.jar "$@"
