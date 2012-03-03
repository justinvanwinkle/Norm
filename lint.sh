#!/bin/bash

find $1 -iname '*.py' | xargs -n1 -P24 lintrunner.sh
