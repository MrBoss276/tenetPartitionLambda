import os
import logging
import pymysql
import datetime
import requests
import re
#
# rds settings
# hosts_str = "tenet-prod-rds-mobin.cqisesdchyjc.ap-south-1.rds.amazonaws.com"
# user_names_str = "admin"
# passwords_str = "mobin123"
# db_names_str = "mobin"
# slack_webhook_url = "http://abcd"

hosts_str = os.environ['HOSTS']
user_names_str = os.environ['USER_NAMES']
passwords_str = os.environ['PASSWORDS']
db_names_str = os.environ['DB_NAMES']
slack_webhook_url = os.environ['SLACK_URL']

hosts = re.split(r'[, ]+', hosts_str)
user_names = re.split(r'[, ]+', user_names_str)
passwords = re.split(r'[, ]+', passwords_str)
db_names = re.split(r'[, ]+', db_names_str)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# print ("SUCCESS: Connection to RDS mysql instance succeeded")
def lambda_handler(event, context):
    global hosts, user_names, passwords, db_names
    hosts = re.split(r'[, ]+', hosts_str)
    user_names = re.split(r'[, ]+', user_names_str)
    passwords = re.split(r'[, ]+', passwords_str)
    db_names = re.split(r'[, ]+', db_names_str)

    if len(hosts) != len(user_names) or len(hosts) != len(passwords) or len(hosts) != len(db_names):
        send_alert_to_slack(["length of all environment variables must be same"], "ERROR")
        return
    n = len(hosts)
    for i in range(n):
        processPartitionForDb(hosts[i], user_names[i], passwords[i], db_names[i])


def processPartitionForDb(rds_host, name, password, db_name):
    """
    This function fetches content from mysql RDS instance
    """
    print("Came inside handler")
    conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=200)
    conn2 = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=200)
    queries = []
    print("got connection")
    with conn.cursor() as cur:
        schema_name = 0
        table_name = 0
        partition_size = 0
        update_field = 0
        last_partition_name = 0
        empty_partitions_required = 0
        current_empty_partitions = 0
        max_partitions_req = 0
        partition_detail_query = "select * from " + db_name + ".partition_details"
        print(partition_detail_query)
        cur.execute(partition_detail_query)
        for row in cur:
            print(row)
            table_name = row[1]
            schema_name = row[2]
            partition_size = row[3]
            empty_partitions_required = row[4]
            update_field = row[5]
            last_partition_name = row[6]
            max_partitions_req = row[7]
            is_time_based = row[8]
            time_in_days = row[9]

            print(table_name)
            print(schema_name)
            print(partition_size)
            print(empty_partitions_required)
            print(max_partitions_req)

            if empty_partitions_required > 1:
                query = "select count(*) from  INFORMATION_SCHEMA.PARTITIONS where table_name='" + table_name + "' and table_schema = '" + schema_name + "' and TABLE_ROWS=0 "
                print(query)
                with conn2.cursor() as cur2:
                    cur2.execute(query)
                    for row in cur2:
                        print(row)
                        current_empty_partitions = row[0]
                        # print ("current_empty_partitons:" + str(current_empty_partitons))

                    query = "select Max(PARTITION_ORDINAL_POSITION) from  INFORMATION_SCHEMA.PARTITIONS where table_name='" + table_name + "' and table_schema = '" + schema_name + "' and TABLE_ROWS>0"
                    print(query)
                    cur2.execute(query)
                    is_empty_table = 0
                    for row in cur2:
                        print(row)
                        if row[0] is None:
                            is_empty_table = 1
                            continue
                        last_completed_partition = row[0]
                        last_but_one_partition = row[0] - 1
                        # print ("last_completed_partition:" + str(last_completed_partition))
                    if is_empty_table:
                        continue
                    query = "select MAX(PARTITION_ORDINAL_POSITION) from  INFORMATION_SCHEMA.PARTITIONS where PARTITION_DESCRIPTION != 'MAXVALUE' AND table_name='" + table_name + "' and table_schema = '" + schema_name + "'"
                    print(query)
                    cur2.execute(query)
                    for row in cur2:
                        print(row)
                        last_non_max_partition = row[0]
                        print("last_non_max_partiton:" + str(last_non_max_partition))

                    actual_empty_partition = last_non_max_partition - last_completed_partition
                    print("actual empty partitions "+str(actual_empty_partition))

                    if empty_partitions_required > actual_empty_partition:
                        last_completed_partition = 0
                        last_but_one_partition = 0
                        last_completed_partition_name = 0
                        last_but_one_partition_name = 0
                        last_completed_partiton_size = 0
                        last_but_one_partition_size = 0
                        last_completed_partition_time = 0
                        last_but_one_partition_time = 0

                        if is_time_based > 0:
                            query = "SELECT PARTITION_NAME, PARTITION_EXPRESSION, PARTITION_DESCRIPTION FROM INFORMATION_SCHEMA.PARTITIONS WHERE PARTITION_ORDINAL_POSITION = " + str(
                                last_non_max_partition) + "  AND TABLE_NAME='" + table_name + "' and table_schema = '" + schema_name + "'"
                            last_partition_name = ""
                            partition_expression = ""
                            last_partition_description = ""
                            print(query)
                            cur2.execute(query)
                            for row in cur2:
                                print(row)
                                last_partition_name = row[0]
                                partition_expression = row[1]
                                last_partition_description = row[2]
                                last_partition_description = last_partition_description.replace('\'', "")
                            date_1 = datetime.datetime.strptime(last_partition_description, "%Y-%m-%d")
                            partition_date = date_1 + datetime.timedelta(days=int(time_in_days))
                            next_partition_date = partition_date.strftime("%Y-%m-%d")
                            next_partition_description = 'p_lt_' + next_partition_date.replace('-', '')
                            query = "ALTER TABLE " + schema_name + "." + table_name + " REORGANIZE PARTITION p_lt_max INTO (PARTITION " + next_partition_description + " VALUES LESS THAN ('" + next_partition_date + "') ENGINE = InnoDB, PARTITION p_lt_max VALUES LESS THAN (MAXVALUE) ENGINE = InnoDB)"
                            print(query)
                            queries.append(query)

            current_filled_partitions = 0
            if max_partitions_req > 0:
                query = "select count(*) from  INFORMATION_SCHEMA.PARTITIONS where table_name='" + table_name + "' and table_schema = '" + schema_name + "' and TABLE_ROWS>0"
                with conn2.cursor() as cur2:
                    cur2.execute(query)
                    for row in cur2:
                        current_filled_partitions = row[0]
                        # print ("current_filled_partitions:" + str(current_filled_partitions))

                if current_filled_partitions > 0:
                    if current_filled_partitions > max_partitions_req:
                        query = "select PARTITION_NAME from  INFORMATION_SCHEMA.PARTITIONS where table_name='" + table_name + "' and table_schema = '" + schema_name + "' and PARTITION_ORDINAL_POSITION=1"
                        with conn2.cursor() as cur2:
                            cur2.execute(query)

                            for row in cur2:
                                partition_name = row[0]
                                # print ("delete partitoin" + str(partition_name))  
                                query_for_drop_command = "ALTER TABLE " + schema_name + "." + table_name + " DROP PARTITION " + str(
                                    partition_name)
                                print(query_for_drop_command)
                                queries.append(query_for_drop_command)

    print(queries)
    if len(queries) > 0:
        send_alert_to_slack(queries, db_name)

    conn.commit()
    conn2.commit()
    cur.close()
    cur2.close()
    conn.close()
    conn2.close()


def send_alert_to_slack(queries, dbName):
    print("slack data ", queries, dbName)
    url = slack_webhook_url
    title = "Partition Query For " + dbName
    for query in queries:
        data = {
            "text": "PARTITION ALERTS",
            "attachments": [{
                "text": title,
                "fields": [{
                    "title": query,
                    "value": "",
                    "short": False
                }]
            }]
        }
        res = requests.post(url, json=data)
        print("slack res : ", res)


# lambda_handler("", "")