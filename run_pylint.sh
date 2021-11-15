#! /bin/bash
pylint --rcfile=./pylint.ini $(git ls-files '*.py')
