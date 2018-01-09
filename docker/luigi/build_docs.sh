#!/bin/bash
set -e

# Additional packages are required to be able to produce PDF documentation (note that these are a 700MB download)
apt-get update && apt-get install -y texlive-latex-recommended texlive-fonts-recommended texlive-latex-extra latexmk

if [ ! -f /usr/local/bin/gosu ]; then
    echo Installing gosu
    apt-get update && apt-get install -y dirmngr  # required on modern Ubuntu
    # See https://github.com/tianon/gosu
    export GOSU_VERSION=1.10
    echo $GOSU_VERSION
    set -x \
        && echo $GOSU_VERSION \
        && dpkgArch="$(dpkg --print-architecture | awk -F- '{ print $NF }')" \
        && curl -Lo /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$dpkgArch" \
        && curl -Lo /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$dpkgArch.asc" \
        && export GNUPGHOME="$(mktemp -d)" \
        && gpg --keyserver hkp://ha.pool.sks-keyservers.net:80 --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4 \
        && gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu \
        && rm -r "$GNUPGHOME" /usr/local/bin/gosu.asc \
        && chmod +x /usr/local/bin/gosu \
        && gosu nobody true
fi

cd docs
gosu luigi make html
gosu luigi make latexpdf
cp _build/latex/*.pdf _build/html/
cd ..
