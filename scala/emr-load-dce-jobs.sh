#!/bin/sh

set -u -x
CLUSTER=${STACK_NAME}-${PLATFORM}-spark-emr
CLUSTER_ID=$(aws emr list-clusters --region=eu-west-1 --active | jq  -r '.[][] | select(.Name == "'${CLUSTER}'") | .Id')

aws s3 cp s3://test-dce-spark/ . --recursive --exclude "*" --include "jar"
aws s3 cp s3://test-dce-spark/emr-load-dce-jobs.sh .
aws s3 cp s3://test-dce-spark/step_jobs.tmpl .

if [ -n ! "${_vardate+set}"];then
_vardate=$(psql -t -c "select to_char(max(start_at), 'YYYY/mm/dd')  from events;" | tr -d ' ')
fi

echo $_vardate

sed -i "s@var_date@$_vardate@g" step_jobs.tmpl

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

if [ -n ! "${_consultemplateversion+set}"];then
   _consultemplateversion=0.19.5
fi
wget https://releases.hashicorp.com/consul-template/${_consultemplateversion}/consul-template_${_consultemplateversion}_linux_386.zip

unzip consul-template_${_consultemplateversion}_linux_386.zip

FILES=$(find . -type f -iname "*.tmpl")

for FILE in $FILES; do
  ./consul-template \
    -consul-ssl="${CONSUL_SECURE}" \
    -consul-addr="${CONSUL_HOST}:${CONSUL_PORT}" \
    -once \
    -template="$FILE:$FILE.json" \
    -vault-renew-token=false \
    -exec "aws emr add-steps --region ${AWS_REGION} --cluster-id $CLUSTER_ID --steps file://$FILE.json"
done

