#!/bin/bash

apt update
apt-get install -y openssh-client

git config --global user.name "darren.foley"
git config --global user.email "darren.foley@ucdconnect.ie"

mkdir -p $HOME/.ssh

ssh-keygen -t rsa

git remote add origin git@github.com:AnalyticsEnthusiast/stedi-streaming-pyspark.git

[ -f "id_rsa*" ] && cp id_rsa* ~/.ssh/
