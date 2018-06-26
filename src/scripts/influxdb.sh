#!/usr/bin/env bash
docker run -d \
  --name docker-influxdb-grafana \
  -p 3003:3003 \
  -p 3004:8083 \
  -p 8086:8086 \
  -p 22022:22 \
  -v /Users/artsemsemianenka/demo/dockerws/influxdb:/var/lib/influxdb \
  -v /Users/artsemsemianenka/demo/dockerws/grafana:/var/lib/grafana \
  philhawthorne/docker-influxdb-grafana:latest