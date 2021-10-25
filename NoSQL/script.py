

# import amazon python SDK and csv stuff
import boto3
import csv

# get an access key and a secret key
s3 = boto3.resource('s3',
    aws_access_key_id='AKIAVEUZ32DQTSHYN4JX',
    aws_secret_access_key='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'
)

#creating our bucket "connorh-datacont" in the Oregon data center
try:
    s3.create_bucket(Bucket='connorh-datacont', CreateBucketConfiguration={
        'LocationConstraint': 'us-west-2'})
except Exception as e:
    print (e)

# make this bucket publicly readable
bucket = s3.Bucket("connorh-datacont") 
bucket.Acl().put(ACL='public-read')

#upload a new object into the bucket
body = open('test.txt', 'rb')

o = s3.Object('connorh-datacont', 'test').put(Body=body )

s3.Object('connorh-datacont', 'test').Acl().put(ACL='public-read')


# create the resource
dyndb = boto3.resource('dynamodb',
    region_name='us-west-2',
    aws_access_key_id='AKIAVEUZ32DQTSHYN4JX',
    aws_secret_access_key='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'
 )

# actually create the table
try: 
    table = dyndb.create_table( 
        TableName='DataTable', 
        KeySchema=[ 
            { 
                'AttributeName': 'PartitionKey', 
                'KeyType': 'HASH' 
            }, 
            { 
                'AttributeName': 'RowKey', 
                'KeyType': 'RANGE' 
            } 
        ], 
        AttributeDefinitions=[ 
            { 
                'AttributeName': 'PartitionKey', 
                'AttributeType': 'S' 
            }, 
            { 
                'AttributeName': 'RowKey', 
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
    #if there is an exception, the table may already exist.   if so... 
    table = dyndb.Table("DataTable") 

#wait for the table to be created
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')


print(table.item_count) 

# read csv data into table
with open('/Users/connorhalloran/Documents/cc_database_hw/experiments.csv', 'r') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')

    # skip first line of csv file
    next(csvf)

    for item in csvf:
        # encode string so it can be hashed
        #item.encode('utf-8')

        print(item)

        #filename = item[4].encode('utf-8');
        

        #body = open('c:\users\farag\documents\datafiles\\'+item[3], 'rb')
        body = open('/Users/connorhalloran/Documents/cc_database_hw/datafiles/'+item[4], 'rb')
        #s3.Object('connorh-datacont', item[4].encode('utf-8')).put(Body=body )
        s3.Object('connorh-datacont', item[4]).put(Body=body )
        md = s3.Object('connorh-datacont', item[4]).Acl().put(ACL='public-read')

        url = " https://s3-us-west-2.amazonaws.com/connorh-datacont/"+item[4]
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
                 'description' : item[4], 'date' : item[2], 'url':url}
        try:
            table.put_item(Item=metadata_item)
        except:
            print("item may already be there or another failure")

# search for an item
response = table.get_item( 
    Key={ 
        'PartitionKey': '3', 
        'RowKey': '-2.93' 
    } 
) 
item = response['Item']
print(item)
