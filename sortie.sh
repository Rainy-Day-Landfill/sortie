#!/bin/bash

# TODO This should be made more clear
export BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

echo $BASEDIR
mount_dir="/opt/pythontechtest"

docker image rm --force "bagira-calypso"
# build the docker file into a local image
docker build -t "bagira-calypso" $BASEDIR

# Decrypt secrets
docker run \
	-it \
	-v ${BASEDIR}:${mount_dir}:Z \
	-v ${HOME}/.aws:/root/.aws:Z \
	--entrypoint=/opt/pythontechtest/sortie.py \
 	"bagira-calypso"
