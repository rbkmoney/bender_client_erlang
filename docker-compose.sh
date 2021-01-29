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
    image: dr2.rbkmoney.com/rbkmoney/machinegun:0da2ffc23221e1e3f8557b03d48d11d2dd18fac0
    command: /opt/machinegun/bin/machinegun foreground
    volumes:
      - ./test/machinegun/config.yaml:/opt/machinegun/etc/config.yaml
      - ./test/machinegun/cookie:/opt/machinegun/etc/cookie
    healthcheck:
      test: "curl http://localhost:8022/"
      interval: 5s
      timeout: 1s
      retries: 20

  bender:
    image: dr2.rbkmoney.com/rbkmoney/bender:cd0ee8faae41f22a40ea119337be2a842e3e9cd8
    command: /opt/bender/bin/bender foreground
    healthcheck:
      test: "curl http://localhost:8022/"
      interval: 5s
      timeout: 1s
      retries: 20
    depends_on:
      - machinegun
EOF
