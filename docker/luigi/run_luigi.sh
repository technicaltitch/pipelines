#!/bin/bash

echo Starting luigid
exec luigid \
    --pidfile /usr/src/app/run/luigi.pid \
    --logdir /usr/src/app/log \
    --state-path /usr/src/app/run/state.picke \
    $*
