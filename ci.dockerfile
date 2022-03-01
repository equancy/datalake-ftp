FROM 641143039263.dkr.ecr.eu-west-3.amazonaws.com/runner/ci-python:3.8

RUN set -ex \
    && apt-get update -q \
    && apt-get install -y -q \
        clamav \
        clamav-daemon \
    && rm -rf /var/lib/apt/lists/*
