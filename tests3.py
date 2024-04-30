import logging
if __name__=="__main__":
    logging.basicConfig(level=logging.INFO)

import boto3
import credentials
import hashlib


# client_response=client.list_buckets()

# for bucket in client_response["Buckets"]:
#     print(bucket)


# Probably works
# response = client.upload_file("tests3.py", credentials.s3testvirtualbucket, "ed_test_1")



# Doesn't work but should
#client.download_file(credentials.s3testvirtualbucket, 'ed_test_1', 'pytestback.py')


def test_put_object(bucket_creds:credentials.Credentials,test_data:bytearray)->bool:
    logging.info("########################\nStarting test_put_object\n--------------------------")

    client=bucket_creds.get_client()
    try:
        response=client.put_object(Body=test_data,
                               Bucket=bucket_creds.bucket,
                               Key="darwin")
    except Exception as e:
        logging.error(f"Exception trying to put object: {e}")
        return False
    
    print(f"\nGet Object Response:\n{response}\n\n")

    

    
    if response['ResponseMetadata']['HTTPStatusCode']!=200:
        logging.error(f"Bad response code : {response.status_code=} trying to put object:\n{response.text=}")
        return False
    


    logging.info("Put object passed OK")

    return True

def get_big_darwin(mbytes:int=20)->bytearray:
    res=b''
    for _ in range(mbytes):# Each darwin is roughly 1MB
        res+=open('darwin.html','rb').read()
    return res

def test_get_object(bucket_creds:credentials.Credentials,target_md5)->bool:
    logging.info("########################\nStarting test_get_object\n--------------------------")
    client=bucket_creds.get_client()
    try:
        response=client.get_object(Bucket=bucket_creds.bucket, Key="darwin")
    except Exception as e:
        logging.error(f"Exception trying to get object: {e}")
        return False
    
    print(f"\nGet Object Response:\n{response}\n\n")
    
    if response['ResponseMetadata']['HTTPStatusCode']!=200:
        logging.error(f"Bad response code : {response.status_code=} trying to get object:\n{response.text=}")
        return False
    
    # Check MD5=5663b926d91c5fc1edd22256a039947a

    contents=response['Body'].read()

    contents_m5=calculate_md5(contents)

    if contents_m5!=target_md5:
        logging.error(f"Bad MD5: {contents_m5=}!={target_md5=}")
        return False

    
    logging.info("Get object passed OK")

    return True


def calculate_md5_from_file(file_path:str)->str:
    contents=open(file_path,'rb').read()
    return calculate_md5(contents)

def calculate_md5(contents:bytearray)->str:
    md5_hash = hashlib.md5()
    res=md5_hash.update(contents)
    return md5_hash.hexdigest()


if __name__=="__main__":
    logging.info("Starting tests....")

    target_content:bytearray=get_big_darwin(mbytes=10)

    target_md5=calculate_md5(target_content)


    passed_put_object=test_put_object(credentials.umbra_demo,target_content)

    passed_get_object=test_get_object(credentials.umbra_demo,target_md5)


