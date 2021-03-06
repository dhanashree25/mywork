#!/bin/sh

set -u -e -x

if [ -z "${var_date-}" ]; then
  var_date=$(psql --tuples-only --command="select to_char(max(start_at), 'YYYY/mm/dd') from event;" || false)
  var_date=$(echo "${var_date}" | tr -d ' ')
  var_path=$(date -d "${var_date}" +year=%Y/month=%m/day=%d)
fi

if [ -z "${var_date-}" ]; then
  var_date=$(date -d "yesterday" +%Y/%m/%d)
  var_path=$(date -d "yesterday" +year=%Y/month=%m/day=%d)
fi

echo "Processing date ${var_date}"

HOST_IP=$(curl --connect-time 5 --max-time 5 --silent http://169.254.169.254/latest/meta-data/local-ipv4 || echo '')

if [ -z "${CONSUL_HOST-}" ]; then
  CONSUL_HOST="${HOST_IP}"
fi
if [ -z "${CONSUL_HOST-}" ]; then
  CONSUL_HOST="127.0.0.1";
fi
if [ -z "${CONSUL_PORT-}" ]; then
  CONSUL_PORT="8500";
fi
if [ -z "${CONSUL_SECURE-}" ]; then
  CONSUL_SECURE="true";
fi

FILES=$(find . -type f -iname "*.tmpl")

var_version=$(cat version)

for FILE in $FILES; do
  sed -i "s@var_date@$var_date@g" "${FILE}"
  sed -i "s@var_version@$var_version@g" "${FILE}"
  sed -i "s@var_path@$var_path@g" "${FILE}"

  consul-template \
    -consul-ssl="${CONSUL_SECURE}" \
    -consul-addr="${CONSUL_HOST}:${CONSUL_PORT}" \
    -consul-retry \
    -consul-retry-attempts=3 \
    -once \
    -template="$FILE:$FILE.json" \
    -vault-renew-token=false

  aws emr add-steps --region=${AWS_REGION} --cluster-id=${EMR_CLUSTER} --steps=file://$FILE.json
done
