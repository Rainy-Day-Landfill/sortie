FROM debian:latest

RUN apt-get update -y \
    && apt-get -y install libsasl2-dev python-dev libldap2-dev libssl-dev libsnmp-dev python3-pip

COPY requirements.txt requirements.txt
RUN /usr/bin/pip3 install -r requirements.txt
