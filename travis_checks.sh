#!/usr/bin/env bash

test_repeat=10

# We use ./gradlew (the gradle wrapper) because travis is using an ancient version of gradle.
# For info on the gradle wrapper see https://docs.gradle.org/current/userguide/gradle_wrapper.html

# Do a complete build and generate jars
# ant
./gradlew jar

# was ant test || { echo "Test $i failed, exiting.." ; exit 1; }
for ((i=0; i<$test_repeat; i++)); do
   ./gradlew test || { echo "Test $i failed, exiting.." ; exit 1; }
done
