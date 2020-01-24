
import boto3
from string import Template
from time import sleep
from pydash import ( get, set_ )

def get_retries():
    return 20

def get_dynamo():
    try:
        dynamo = boto3.client('dynamodb')
        return (True, dynamo, None)
    except Exception as e:
        return (False, None, e)

def get_table_status(dynamo, table_name):
    try:
        result = dynamo.describe_table(TableName=table_name)
        status = get(result, 'Table.TableStatus')
        return (True, status, None)
    except dynamo.exceptions.ResourceNotFoundException as e:
        return (True, 'DOESNOTEXIST', None)
    except Exception as e:
        return (False, None, e)
        
def create_table(dynamo, table_name):
    try:
        result = dynamo.create_table(
            AttributeDefinitions=[
                {
                    'AttributeName': 'uuid',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'value',
                    'AttributeType': 'N'
                },
                {
                    'AttributeName': 'profit',
                    'AttributeType': 'N'
                },
                {
                    'AttributeName': 'type',
                    'AttributeType': 'S'
                },
            ],
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'uuid',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'value',
                    'KeyType': 'RANGE'
                },
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'value-index',
                    'KeySchema': [
                        {
                            'AttributeName': 'value',
                            'KeyType': 'HASH'
                        },
                        {
                            'AttributeName': 'profit',
                            'KeyType': 'RANGE'
                        },
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL'
                    },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 10,
                        'WriteCapacityUnits': 10
                    }
                },
                {
                    'IndexName': 'profit-index',
                    'KeySchema': [
                        {
                            'AttributeName': 'profit',
                            'KeyType': 'HASH'
                        },
                        {
                            'AttributeName': 'value',
                            'KeyType': 'RANGE'
                        },
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL'
                    },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 10,
                        'WriteCapacityUnits': 10
                    }
                },
                {
                    'IndexName': 'type-index',
                    'KeySchema': [
                        {
                            'AttributeName': 'type',
                            'KeyType': 'HASH'
                        },
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL'
                    },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 10,
                        'WriteCapacityUnits': 10
                    }
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        return (True, result, None)
    except Exception as e:
        return (False, None, e)


def wait_on_desired_status(dynamo, table_name, desired_status, max_time, current_time):
    timeout = 5
    if current_time > max_time:
        s = Template('wait_on_desired_status, tableName: $tableName never became $desiredStatus, waited $currentTime of $maxTime')
        return (False, None, Exception(s.substitute(tableName=table_name, desiredStatus=desired_status, currentTime=current_time, maxTime=max_time)))

    ok, status, error = get_table_status(dynamo, table_name)
    if ok == False:
        return (False, None, error)

    if status == desired_status:
        return (True, status, None)

    print('Status ' + status + ' is not ' + desired_status + '. Trying attempt ' + str(current_time + 1) + ' of ' + str(max_time) + ' in 5 seconds...')
    sleep(timeout)
    return wait_on_desired_status(dynamo, table_name, desired_status, max_time, current_time + 1)

def attempt_create_table(dynamo, table_name):
    ok, status, error = get_table_status(dynamo, table_name)
    if ok == False:
        print("get_table_status error:", error)
        return (False, None, error)

    if status == 'ACTIVE':
        print("table already exists, success")
        return (True, None, None)

    if status == 'DOESNOTEXIST':
        ok, create_result, error = create_table(dynamo, table_name)
        if ok == False:
            print("create_table error:", error)
            return (False, None, error)
        
        ok, _, error = wait_on_desired_status(dynamo, table_name, 'ACTIVE', get_retries(), 0)
        if ok == False:
            print("wait_on_desired_status error:", error)
            return (False, None, error)

        return (True, None, None)

    if status == 'CREATING':
        ok, _, error = wait_on_desired_status(dynamo, table_name, 'ACTIVE', get_retries(), 0)
        if ok == False:
            print("wait_on_desired_status error:", error)
            return (False, None, error)

        return (True, None, None)

    return (False, None, Exception('Table is inaccessible to delete.'))

def get_table_arn(dynamo, table_name):
    try:
        result = dynamo.describe_table(TableName=table_name)
        arn = get(result, 'Table.TableArn', '???')
        if arn == '???':
            return (False, None, Exception('Describe table failed to give us an Arn in the result.'))

        return (True, arn, None)
    except Exception as e:
        return (False, None, e)

def handler(event):
    table_name = get(event, 'tableName')
    if table_name == None or table_name == '':
        blank_error_text = 'tableName not found in event or it was blank'
        print(blank_error_text)
        raise Exception(blank_error_text)

    ok, dynamo, error = get_dynamo()
    if ok == False:
        print("Failed to get dynamo:", error)
        raise error
    
    ok, _, error = attempt_create_table(dynamo, table_name)
    if ok == False:
        print("failed to create_table:", error)
        raise error

    ok, arn, error = get_table_arn(dynamo, table_name)
    if ok == False:
        print("Successfully created table, but failed to get the Arn of it:", error)
        raise error

    updated_event = set_(event, 'tableArn', arn)
    print("Done! updated_event:", updated_event)
    return updated_event

if __name__ == "__main__":
   handler({'tableName': 'asteroids'})