#!/bin/bash
cat <<EOF
version: '2.1'
services:

  ${SERVICE_NAME}:
    image: ${BUILD_IMAGE}
    volumes:
      - .:$PWD
      - $HOME/.cache:/home/$UNAME/.cache
    working_dir: $PWD
    command: /sbin/init
    depends_on:
      bender:
        condition: service_healthy
    mem_limit: 256M

  machinegun:
    image: dr2.rbkmoney.com/rbkmoney/machinegun:aec434f47029dbd81762e10de04c9422e3c93e5e
    command: /opt/machinegun/bin/machinegun foreground
    volumes:
      - ./test/machinegun/config.yaml:/opt/machinegun/etc/config.yaml
    healthcheck:
      test: "curl http://localhost:8022/"
      interval: 5s
      timeout: 1s
      retries: 20

  bender:
    image: dr2.rbkmoney.com/rbkmoney/bender:90ca71b80ac280ed205d0568e214c7524e6cb328
    command: /opt/bender/bin/bender foreground
    healthcheck:
      test: "curl http://localhost:8022/"
      interval: 5s
      timeout: 1s
      retries: 20
    depends_on:
      - machinegun
EOF
