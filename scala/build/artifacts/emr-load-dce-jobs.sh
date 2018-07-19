#!/bin/sh

set -u -e -x

if [ -z "${var_date-}" ]; then
  var_date=$(psql --tuples-only --command="select to_char(max(start_at), 'YYYY/mm/dd') from event;" || false)
  var_date=$(echo "${var_date}" | tr -d ' ')
fi

if [ -z "${var_date-}" ]; then
  var_date=$(date -d "yesterday" +%Y/%m/%d)
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

consul_template_version=0.19.5
consul_file="consul-template.zip"

wget -O "${consul_file}" https://releases.hashicorp.com/consul-template/${consul_template_version}/consul-template_${consul_template_version}_linux_386.zip
unzip -o "${consul_file}"
rm -f "${consul_file}"

FILES=$(find . -type f -iname "*.tmpl")

var_version=$(cat version)

for FILE in $FILES; do
  sed -i "s@var_date@$var_date@g" "${FILE}"
  sed -i "s@var_version@$var_version@g" "${FILE}"

  ./consul-template \
    -consul-ssl="${CONSUL_SECURE}" \
    -consul-addr="${CONSUL_HOST}:${CONSUL_PORT}" \
    -consul-retry \
    -consul-retry-attempts=3 \
    -once \
    -template="$FILE:$FILE.json" \
    -vault-renew-token=false

  aws emr add-steps --region=${AWS_REGION} --cluster-id=${EMR_CLUSTER} --steps=file://$FILE.json
done
