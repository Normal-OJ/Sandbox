#!/bin/bash
# c_cpp
docker build -t c_cpp -f c_cpp_dockerfile . --no-cache
# python3
docker build -t python3 -f python3_dockerfile . --no-cache
