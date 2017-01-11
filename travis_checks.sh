#!/usr/bin/env bash

test_repeat=10

ant

for ((i=0; i<$test_repeat; i++)); do
   ant test || { echo "Test $i failed, exiting.." ; exit 1; }
done
