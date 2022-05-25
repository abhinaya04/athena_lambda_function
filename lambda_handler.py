import boto3
import datetime
import time

#This should be the name of your Athena database
ATHENA_DATABASE = "athena_database"

# #This should be the name of your Athena database table
ATHENA_TABLE = "alb_logs_partitioned_2022_05"

#This is the Amazon S3 bucket name the logs queried from:
LOG_BUCKET = "mytrail-singapore"

# #AWS Account number for the Amazon S3 path to your access logs
AWS_ACCOUNT_ID = "xxxxxxxxxxxxxx"

#This is the Amazon S3 bucket name for the Athena Query results:
OUTPUT_LOG_BUCKET = "athena-output-bucket-2505"

#Define region
REGION = "ap-southeast-1"


RETRY_COUNT = 50

#Getting the current date and splitting into variables to use in queries below
CURRENT_DATE = datetime.datetime.today()
DATEFORMATTED = (CURRENT_DATE.isoformat())
ATHENA_YEAR = str(DATEFORMATTED[:4])
ATHENA_MONTH = str(DATEFORMATTED[5:7])
ATHENA_DAY = str(DATEFORMATTED[8:10])

#location for the Athena query results
OUTPUT_LOCATION = "s3://"+OUTPUT_LOG_BUCKET+"/DailyAthenaLogs/ELB/"+str(CURRENT_DATE.isoformat())
LOCATION = f' location "s3://{LOG_BUCKET}/AWSLogs/{AWS_ACCOUNT_ID}/elasticloadbalancing/{REGION}/{ATHENA_YEAR}/{ATHENA_MONTH}/{ATHENA_DAY}/"'

QUERY_1 = f' CREATE DATABASE {ATHENA_DATABASE}'

QUERY_2 = f"CREATE EXTERNAL TABLE IF NOT EXISTS {ATHENA_TABLE} ( type string,time string,elb string,client_ip string,client_port int,target_ip string,target_port int,request_processing_time double,target_processing_time double,response_processing_time double,elb_status_code int,target_status_code string,received_bytes bigint,sent_bytes bigint,request_verb string,request_url string,request_proto string,user_agent string,ssl_cipher string,ssl_protocol string,target_group_arn string,trace_id string,domain_name string,chosen_cert_arn string,matched_rule_priority string,request_creation_time string,actions_executed string,redirect_url string,lambda_error_reason string,target_port_list string,target_status_code_list string,classification string,classification_reason string ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe' WITH SERDEPROPERTIES ('serialization.format' = '1', 'input.regex' = '([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*)[:-]([0-9]*) ([-.0-9]*) ([-.0-9]*) ([-.0-9]*) (|[-0-9]*) (-|[-0-9]*) ([-0-9]*) ([-0-9]*) \"([^ ]*) (.*) (- |[^ ]*)\" \"([^\"]*)\" ([A-Z0-9-_]+) ([A-Za-z0-9.-]*) ([^ ]*) \"([^\"]*)\" \"([^\"]*)\" \"([^\"]*)\" ([-.0-9]*) ([^ ]*) \"([^\"]*)\" \"([^\"]*)\" \"([^ ]*)\" \"([^\s]+?)\" \"([^\s]+)\" \"([^ ]*)\" \"([^ ]*)\"') {LOCATION}"
	
QUERY_3 = f"SELECT * FROM {ATHENA_DATABASE}.{ATHENA_TABLE} where elb='app/test/59136d6fc878fd89' and (elb_status_code = 503) and  time > to_iso8601(current_timestamp - interval '7' day)"

#Defining function to generate query status for each query
def query_stat_fun(query, response):
    print('In Query Stat Function')
    client = boto3.client('athena')
    query_execution_id = response['QueryExecutionId']
    print(query_execution_id +' : '+query)
    for i in range(1, 1 + RETRY_COUNT):
        query_status = client.get_query_execution(QueryExecutionId=query_execution_id)
        query_fail_status = query_status['QueryExecution']['Status']
        query_execution_status = query_fail_status['State']

        if query_execution_status == 'SUCCEEDED':
            print("STATUS:" + query_execution_status)
            break

        if query_execution_status == 'FAILED':
            print(query_fail_status)

        else:
            print("STATUS:" + query_execution_status)
            time.sleep(i)
    else:
        client.stop_query_execution(QueryExecutionId=query_execution_id)
        raise Exception('Maximum Retries Exceeded')

def lambda_handler(query, context):
    client = boto3.client('athena')

    queries = [QUERY_1,QUERY_2, QUERY_3]
    for query in queries:
        print('Starting the Query Execution')
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': ATHENA_DATABASE },
            ResultConfiguration={
                'OutputLocation': OUTPUT_LOCATION })
        query_stat_fun(query, response)
