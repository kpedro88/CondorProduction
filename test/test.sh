#!/bin/bash

echo "This is a test job. It will print the current directory, contents, and environment."
echo ""
echo "Current directory:"
echo $PWD
echo ""
echo "Current directory listing:"
ls -ltha
echo ""
echo "Current environment:"
printenv

