# Zale de Jong
# Professor Farag
# 21 October 2021
# HW3 - NoSQL

import boto3
import csv

s3 = boto3.resource('s3',
    aws_access_key_id='AKIA2Q57XLYONWR22L26',
    aws_secret_access_key='SECRET_KEY_HERE'
)

try:
    s3.create_bucket(Bucket='14-848-hw3-zale',
        CreateBucketConfiguration={'LocationConstraint': 'us-east-2'})
except Exception as e:
    print (e)

bucket = s3.Bucket("14-848-hw3-zale")
bucket.Acl().put(ACL='public-read')

#upload a new object into the bucket
body = open('/mnt/c/Users/zale/Documents/CMU/14-848 Cloud/HW3/exp1.csv', 'rb')
o = s3.Object('14-848-hw3-zale', 'exp1').put(Body=body)
s3.Object('14-848-hw3-zale', 'exp1').Acl().put(ACL='public-read')

body = open('/mnt/c/Users/zale/Documents/CMU/14-848 Cloud/HW3/exp2.csv', 'rb')
o = s3.Object('14-848-hw3-zale', 'exp2').put(Body=body)
s3.Object('14-848-hw3-zale', 'exp2').Acl().put(ACL='public-read')

body = open('/mnt/c/Users/zale/Documents/CMU/14-848 Cloud/HW3/exp3.csv', 'rb')
o = s3.Object('14-848-hw3-zale', 'exp3').put(Body=body)
s3.Object('14-848-hw3-zale', 'exp3').Acl().put(ACL='public-read')

dyndb = boto3.resource('dynamodb', region_name='us-east-2',
    aws_access_key_id='AKIA2Q57XLYONWR22L26',
    aws_secret_access_key='SECRET_KEY_HERE')

try:
    table = dyndb.create_table(
        TableName='experiments_data',
        KeySchema=[
            {
                'AttributeName': 'id',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'temp',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'temp',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except Exception as e:
    print (e)
    #if there is an exception, the table may already exist. if so...
    table = dyndb.Table("experiments_data")

#wait for the table to be created
table.meta.client.get_waiter('table_exists').wait(TableName='experiments_data')
print(table.item_count)

# reading the csv file
with open('/mnt/c/Users/zale/Documents/CMU/14-848 Cloud/HW3/experiments.csv',
        'r') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    iter_csvf = iter(csvf)
    next(csvf)
    for item in csvf:
        print(item)
        body = open('/mnt/c/Users/zale/Documents/CMU/14-848 Cloud/HW3/'+item[4],
            'rb')
        s3.Object('14-848-hw3-zale', item[4]).put(Body=body)
        md = s3.Object('14-848-hw3-zale', item[4]).Acl().put(ACL='public-read')

        url = " https://s3-us-east-2.amazonaws.com/14-848-hw3-zale/"+item[4]
        metadata_item = {'id': item[0], 'temp': item[1],
        'conductivity' : item[2], 'concentration' : item[3], 'url' : url}
        try:
            table.put_item(Item=metadata_item)
        except:
            print("item may already be there or another failure")

# ['experiment1', '1', '3/15/2002', 'exp1', 'this is the comment']
# ['experiment1', '2', '3/15/2002', 'exp2', 'this is the comment2']
# ['experiment2', '3', '3/16/2002', 'exp3', 'this is the comment3']
# ['experiment3', '4', '3/16/2002', 'exp4', 'this is the comment233']

response = table.get_item(
    Key={
        'id': '1',
        'temp': '-1'
    }
)
item = response['Item']
print(item)
