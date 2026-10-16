#!/bin/sh
# 1) один раз: принять условия Lakekeeper
curl -s -X POST http://localhost:8181/management/v1/bootstrap \
  -H 'Content-Type: application/json' -d '{"accept-terms-of-use": true}'
# 2) создать warehouse, указывающий на бакет MinIO
curl -s -X POST http://localhost:8181/management/v1/warehouse \
  -H 'Content-Type: application/json' -d '{
  "warehouse-name": "lakehouse",
  "storage-profile": {"type":"s3","bucket":"lakehouse","endpoint":"http://seaweedfs:8333",
    "region":"us-east-1","path-style-access":true,"flavor":"s3-compat","sts-enabled":false},
  "storage-credential": {"type":"s3","credential-type":"access-key",
    "aws-access-key-id":"admin","aws-secret-access-key":"admin12345"}}'
