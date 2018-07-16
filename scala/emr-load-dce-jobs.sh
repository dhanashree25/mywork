#!/bin/sh

set -u -x
BUCKET=${STACK_NAME}-${PLATFORM}-dce-test-emr
CLUSTER=${STACK_NAME}-${PLATFORM}-spark-emr
CLUSTER_ID=$(aws emr list-clusters --region=eu-west-1 --active | jq  -r '.[][] | select(.Name == "'${CLUSTER}'") | .Id')

aws s3 cp s3://test-dce-spark/ . --recursive --exclude "*" --include "jar"
#aws s3 cp s3://test-dce-spark/emr-load-dce-jobs.sh .

#if [ -z '$var_date+x' ];then
var_date=$(psql -t -c "select to_char(max(start_at), 'YYYY/mm/dd')  from live_play;" | tr -d ' ')
#fi
#var_date=$(date -d '-1 day' '+%Y/%d/%m')
echo $var_date

sed -i "s@var_date@$var_date@g" step_vod.json

aws emr add-steps --region ${region} --cluster-id ${CLUSTER_ID} --steps file://step_vod.json
