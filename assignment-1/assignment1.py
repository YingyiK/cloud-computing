# Programming Assignment 1

#   1. Create two IAM roles. One is called `Dev` and the other `User`

#   2. Attach IAM policies to the roles so that `Dev` has full access to S3, while `User` can only list/get S3 buckets and objects

#   3. Create an IAM user (name is up to you)

#   4. Use the created user to assume the `Dev` role and create the following buckets and objects

#    -  A bucket with a unique name (up to you what you want to call it)

#    -  Create object `assignment1.txt` in the bucket, by uploading a txt file whose content is a string `Empty Assignment 1`

#    -  Create object `assignment2.txt` in the bucket, by uploading another txt file whose content is `Empty Assignment 2`

#    -  Create object `recording1.jpg` in the bucket, by uploading a small pic (your pick).

#   5. Quit the `Dev` role and now assume the `User` role, and do the following

#    -  Find all the objects whose key has the prefix `assignment` and compute the total size of those objects.

#   6. Quit the `User` role and now assume the `Dev` role, then delete the all the objects and the bucket.


import boto3
import json
import time
from io import BytesIO
from PIL import Image

#initialize IAM client
iam = boto3.client('iam')
sts = boto3.client('sts')

USER_NAME = 'Kendrick-Lamar'
BUCKET_PREFIX = 'cloud-assignment1-bucket'
DEV_ROLE_NAME = 'Dev'
USER_ROLE_NAME = 'User'
REGION = 'us-west-2'

account_id = sts.get_caller_identity()['Account']
print(f"\nYour AWS Account ID: {account_id}")
print(f"Region: {REGION}\n")

#   1. Create two IAM roles. One is called `Dev` and the other `User`

#   2. Attach IAM policies to the roles so that `Dev` has full access to S3, while `User` can only list/get S3 buckets and objects
# create Dev role and attach AmazonS3FullAccess policy
def create_dev_role():

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": {"AWS": f"arn:aws:iam::{account_id}:root"}, "Action": "sts:AssumeRole"}]
    }

    try:
        dev_role = iam.create_role(
            RoleName=DEV_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description='Dev role with full S3 access'
        )
        print(f"✓ Dev role created")

        iam.attach_role_policy(
            RoleName=DEV_ROLE_NAME,
            PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess'
        )
        print(f"✓ AmazonS3FullAccess policy attached")
    except iam.exceptions.EntityAlreadyExistsException:
        dev_role = iam.get_role(RoleName=DEV_ROLE_NAME)
    return dev_role['Role']['Arn']

# create User role and attach AmazonS3ReadOnlyAccess policy
def create_user_role():

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": {"AWS": f"arn:aws:iam::{account_id}:root"}, "Action": "sts:AssumeRole"}]
    }

    try:
        user_role = iam.create_role(
            RoleName=USER_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description='User role with list/get S3 buckets and objects'
        )
        print(f"✓ User role created")

        iam.attach_role_policy(
            RoleName=USER_ROLE_NAME,
            PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
        )
        print(f"✓ AmazonS3ReadOnlyAccess policy attached")
    except iam.exceptions.EntityAlreadyExistsException:
        user_role = iam.get_role(RoleName=USER_ROLE_NAME)
    return user_role['Role']['Arn']

#   3. Create an IAM user (name is up to you)
# create IAM user and attach AssumeRolePolicy policy
def create_iam_user():
    try:
        user = iam.create_user(UserName=USER_NAME)
        print(f"✓ User created")
        
        permission_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "sts:AssumeRole",
                "Resource": [  
                    f"arn:aws:iam::{account_id}:role/Dev",
                    f"arn:aws:iam::{account_id}:role/User"
                ]
            }]
        }
        
        iam.put_user_policy(
            UserName=USER_NAME,
            PolicyName='AssumeRolePolicy',
            PolicyDocument=json.dumps(permission_policy)
        )
        print(f"✓ User can assume Dev and User roles")
        
    except iam.exceptions.EntityAlreadyExistsException:
        print("✗ User already exists")
        user = iam.get_user(UserName=USER_NAME)
        # Ensure the policy is attached even if user already exists
        try:
            permission_policy = {
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Action": "sts:AssumeRole",
                    "Resource": [  
                        f"arn:aws:iam::{account_id}:role/Dev",
                        f"arn:aws:iam::{account_id}:role/User"
                    ]
                }]
            }
            iam.put_user_policy(
                UserName=USER_NAME,
                PolicyName='AssumeRolePolicy',
                PolicyDocument=json.dumps(permission_policy)
            )
            print(f"✓ User policy updated")
        except Exception as e:
            print(f"  Note: Policy may already exist")
    return user['User']['Arn']


#   4. Use the created user to assume the `Dev` role and create the following buckets and objects
#    -  A bucket with a unique name (up to you what you want to call it)

#    -  Create object `assignment1.txt` in the bucket, by uploading a txt file whose content is a string `Empty Assignment 1`

#    -  Create object `assignment2.txt` in the bucket, by uploading another txt file whose content is `Empty Assignment 2`

#    -  Create object `recording1.jpg` in the bucket, by uploading a small pic (your pick).

# Function to create access keys for the user
def create_user_access_keys(username):
    try:
        # Check if user already has access keys
        existing_keys = iam.list_access_keys(UserName=username)
        if existing_keys['AccessKeyMetadata']:
            # Delete existing keys first (we can't retrieve secret key)
            for key_metadata in existing_keys['AccessKeyMetadata']:
                iam.delete_access_key(
                    UserName=username,
                    AccessKeyId=key_metadata['AccessKeyId']
                )
            print(f"✓ Deleted existing access keys for user {username}")
        
        # Create new access keys
        response = iam.create_access_key(UserName=username)
        access_key_id = response['AccessKey']['AccessKeyId']
        secret_access_key = response['AccessKey']['SecretAccessKey']
        print(f"✓ Access keys created for user {username}")
        
        # Wait for IAM propagation (access keys need a moment to become active)
        print("  Waiting for IAM propagation...")
        time.sleep(10)  # Wait 10 seconds for IAM changes to propagate
        
        return access_key_id, secret_access_key
    except Exception as e:
        print(f"✗ Error creating access keys: {e}")
        raise

# Function to assume the Dev role using user credentials
def assume_dev_role(access_key_id, secret_access_key):
    # Create STS client with user credentials
    user_sts = boto3.client(
        'sts',
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name=REGION
    )
    
    # Assume the Dev role with retry logic
    role_arn = f"arn:aws:iam::{account_id}:role/{DEV_ROLE_NAME}"
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            response = user_sts.assume_role(
                RoleArn=role_arn,
                RoleSessionName='DevRoleSession'
            )
            credentials = response['Credentials']
            print(f"✓ Successfully assumed Dev role")
            return credentials
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"  Retry {attempt + 1}/{max_retries} after {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                print(f"✗ Failed to assume Dev role after {max_retries} attempts: {e}")
                raise

# Function to assume the User role using user credentials
def assume_user_role(access_key_id, secret_access_key):
    # Create STS client with user credentials
    user_sts = boto3.client(
        'sts',
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name=REGION
    )
    
    # Assume the User role with retry logic
    role_arn = f"arn:aws:iam::{account_id}:role/{USER_ROLE_NAME}"
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            response = user_sts.assume_role(
                RoleArn=role_arn,
                RoleSessionName='UserRoleSession'
            )
            credentials = response['Credentials']
            print(f"✓ Successfully assumed User role")
            return credentials
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"  Retry {attempt + 1}/{max_retries} after {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                print(f"✗ Failed to assume User role after {max_retries} attempts: {e}")
                raise

# Function to create bucket and upload objects using assumed role credentials
def create_bucket_and_upload_objects(credentials):
    # Create S3 client with assumed role credentials
    s3_assumed = boto3.client(
        's3',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
        region_name=REGION
    )
    
    # 1. Create a bucket with a unique name
    unique_bucket_name = f"{BUCKET_PREFIX}-{int(time.time())}"
    try:
        s3_assumed.create_bucket(
            Bucket=unique_bucket_name,
            CreateBucketConfiguration={'LocationConstraint': REGION}
        )
        print(f"✓ Bucket '{unique_bucket_name}' created")
    except s3_assumed.exceptions.BucketAlreadyExists:
        print(f"✗ Bucket '{unique_bucket_name}' already exists")
    except Exception as e:
        # If bucket already exists globally, try a different name
        unique_bucket_name = f"{BUCKET_PREFIX}-{account_id}-{int(time.time())}"
        s3_assumed.create_bucket(
            Bucket=unique_bucket_name,
            CreateBucketConfiguration={'LocationConstraint': REGION}
        )
        print(f"✓ Bucket '{unique_bucket_name}' created")
    
    # 2. Create and upload assignment1.txt
    assignment1_content = "Empty Assignment 1"
    s3_assumed.put_object(
        Bucket=unique_bucket_name,
        Key='assignment1.txt',
        Body=assignment1_content.encode('utf-8'),
        ContentType='text/plain'
    )
    print(f"✓ Uploaded assignment1.txt")
    
    # 3. Create and upload assignment2.txt
    assignment2_content = "Empty Assignment 2"
    s3_assumed.put_object(
        Bucket=unique_bucket_name,
        Key='assignment2.txt',
        Body=assignment2_content.encode('utf-8'),
        ContentType='text/plain'
    )
    print(f"✓ Uploaded assignment2.txt")
    
    # 4. Create a small test image and upload as recording1.jpg
    # Create a simple 100x100 pixel image
    img = Image.new('RGB', (100, 100), color='lightblue')
    img_buffer = BytesIO()
    img.save(img_buffer, format='JPEG')
    img_buffer.seek(0)
    
    s3_assumed.put_object(
        Bucket=unique_bucket_name,
        Key='recording1.jpg',
        Body=img_buffer.getvalue(),
        ContentType='image/jpeg'
    )
    print(f"✓ Uploaded recording1.jpg")
    
    return unique_bucket_name

#   5. Quit the `Dev` role and now assume the `User` role, and do the following

#    -  Find all the objects whose key has the prefix `assignment` and compute the total size of those objects.

# Function to find objects with prefix 'assignment' and calculate total size using User role
def find_assignment_objects_and_calculate_size(credentials, bucket_name):
    # Create S3 client with assumed User role credentials (read-only)
    s3_user = boto3.client(
        's3',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
        region_name=REGION
    )
    
    # List all objects with prefix 'assignment'
    total_size = 0
    assignment_objects = []
    
    try:
        paginator = s3_user.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix='assignment')
        
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    size = obj['Size']
                    assignment_objects.append({'key': key, 'size': size})
                    total_size += size
        
        print(f"\n✓ Found {len(assignment_objects)} object(s) with prefix 'assignment':")
        for obj in assignment_objects:
            print(f"  - {obj['key']}: {obj['size']} bytes")
        print(f"✓ Total size of objects with prefix 'assignment': {total_size} bytes ({total_size / 1024:.2f} KB)")
        
    except Exception as e:
        print(f"✗ Error listing objects: {e}")
        raise
    
    return total_size, assignment_objects

#   6. Quit the `User` role and now assume the `Dev` role, then delete the all the objects and the bucket.

# Function to delete all objects and the bucket using Dev role
def delete_all_objects_and_bucket(credentials, bucket_name):
    # Create S3 client with assumed Dev role credentials (full access)
    s3_dev = boto3.client(
        's3',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
        region_name=REGION
    )
    
    try:
        # List and delete all objects in the bucket
        print(f"\n=== Deleting all objects in bucket '{bucket_name}' ===")
        paginator = s3_dev.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name)
        
        objects_deleted = 0
        for page in pages:
            if 'Contents' in page:
                objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
                if objects_to_delete:
                    s3_dev.delete_objects(
                        Bucket=bucket_name,
                        Delete={'Objects': objects_to_delete}
                    )
                    objects_deleted += len(objects_to_delete)
                    print(f"✓ Deleted {len(objects_to_delete)} object(s)")
        
        if objects_deleted == 0:
            print("  No objects found to delete")
        
        # Delete the bucket
        print(f"\n=== Deleting bucket '{bucket_name}' ===")
        s3_dev.delete_bucket(Bucket=bucket_name)
        print(f"✓ Bucket '{bucket_name}' deleted successfully")
        
    except s3_dev.exceptions.NoSuchBucket:
        print(f"✗ Bucket '{bucket_name}' does not exist")
    except Exception as e:
        print(f"✗ Error deleting bucket/objects: {e}")
        raise

# Main execution function
def main():
    print("\n=== Creating IAM Roles and User ===")
    create_dev_role()
    create_user_role()
    create_iam_user()
    
    print("\n=== Assuming Dev Role and Creating S3 Resources ===")
    
    # Create access keys for the user
    access_key_id, secret_access_key = create_user_access_keys(USER_NAME)
    
    # Assume the Dev role
    credentials = assume_dev_role(access_key_id, secret_access_key)
    
    # Create bucket and upload objects
    bucket_name = create_bucket_and_upload_objects(credentials)
    
    print(f"\n✓ All tasks completed successfully!")
    print(f"  Bucket name: {bucket_name}")
    print(f"  Objects created: assignment1.txt, assignment2.txt, recording1.jpg")
    
    # 5. Quit Dev role and assume User role, find objects with prefix 'assignment' and calculate total size
    print("\n=== Quitting Dev Role and Assuming User Role ===")
    user_credentials = assume_user_role(access_key_id, secret_access_key)
    
    print("\n=== Finding objects with prefix 'assignment' and calculating total size ===")
    find_assignment_objects_and_calculate_size(user_credentials, bucket_name)
    
    # 6. Quit User role and assume Dev role, then delete all objects and bucket
    print("\n=== Quitting User Role and Assuming Dev Role ===")
    dev_credentials = assume_dev_role(access_key_id, secret_access_key)
    
    # Ask for confirmation before cleanup
    print(f"\n  WARNING: About to delete all objects and bucket '{bucket_name}'")
    confirmation = input("Type 'yes' to confirm cleanup, or anything else to skip: ")
    
    if confirmation.lower() == 'yes':
        delete_all_objects_and_bucket(dev_credentials, bucket_name)
        print(f"\n✓ All tasks completed successfully!")
    else:
        print(f"\n Cleanup skipped. Bucket '{bucket_name}' and its objects remain.")
        print(f"✓ Tasks completed (without cleanup)")

if __name__ == "__main__":
    main()