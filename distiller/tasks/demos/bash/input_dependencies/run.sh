#!/bin/bash

read pipe_input

echo "Hello World"
echo $pipe_input
echo $1
cat $1

(>&2 echo "This is a test error")

#exit 1
