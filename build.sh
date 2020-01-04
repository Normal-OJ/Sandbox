#!/bin/bash

# sandbox binary
rm -f sandbox
wget https://github.com/Normal-OJ/C-Sandbox/releases/latest/download/sandbox
chmod +x sandbox
# c_cpp
docker build -t noj-c-cpp -f c_cpp_dockerfile . --no-cache
# python3
docker build -t noj-py3 -f python3_dockerfile . --no-cache


