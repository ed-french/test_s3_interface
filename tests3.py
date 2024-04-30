import logging
if __name__=="__main__":
    logging.basicConfig(level=logging.INFO)

import boto3
import credentials
import hashlib
import os


# client_response=client.list_buckets()

# for bucket in client_response["Buckets"]:
#     print(bucket)


# Probably works
# response = client.upload_file("tests3.py", credentials.s3testvirtualbucket, "ed_test_1")



# Doesn't work but should
#client.download_file(credentials.s3testvirtualbucket, 'ed_test_1', 'pytestback.py')


def test_list_objects(bucket_creds:credentials.Credentials)->bool:
    logging.info("########################\nStarting test_list_objects\n--------------------------")
    client=bucket_creds.get_client()
    try:
        response=client.list_objects(Bucket=bucket_creds.bucket)
    except Exception as e:
        logging.error(f"Exception trying to list objects: {e}")
        return False
    
    if response['ResponseMetadata']['HTTPStatusCode']!=200:
        logging.error(f"Bad response code : {response.status_code=} trying to list objects:\n{response.text=}")     
        return False

def test_list_objects_v2(bucket_creds:credentials.Credentials)->bool:
    logging.info("########################\nStarting test_list_objects_v2\n--------------------------")
    client=bucket_creds.get_client()
    try:
        response=client.list_objects_v2(Bucket=bucket_creds.bucket)
    except Exception as e:
        logging.error(f"Exception trying to list objects v2: {e}")
        return False
    
    if response['ResponseMetadata']['HTTPStatusCode']!=200:
        logging.error(f"Bad response code : {response.status_code=} trying to list objects v2:\n{response.text=}")     
        return False


    logging.info("List objects v2 passed OK")
    return True


def test_delete_object(bucket_creds:credentials.Credentials,key:str)->bool:
    logging.info("########################\nStarting test_delete_object\n--------------------------")
    client=bucket_creds.get_client()
    try:
        response=client.delete_object(Bucket=bucket_creds.bucket, Key=key)
    except Exception as e:
        logging.error(f"Exception trying to delete object: {e}")
        return False
    if response['ResponseMetadata']['HTTPStatusCode']!=204:
        logging.error(f"Bad response code : {response.status_code=} trying to delete object:\n{response.text=}")
        return False
    logging.info("Delete object passed OK")
    return True



def test_list_buckets(bucket_creds:credentials.Credentials)->bool:
    logging.info("########################\nStarting test_list_buckets\n--------------------------")    
    client=bucket_creds.get_client()
    try:
        response=client.list_buckets()
    except Exception as e:
        logging.error(f"Exception trying to list buckets: {e}") 
        return False
    if response['ResponseMetadata']['HTTPStatusCode']!=200:
        logging.error(f"Bad response code : {response.status_code=} trying to list buckets:\n{response.text=}")
        return False
    logging.info("List buckets passed OK")
    return True


def test_upload_file(bucket_creds:credentials.Credentials,filename:str)->bool:
    logging.info("########################\nStarting test_upload_file\n--------------------------")
    client=bucket_creds.get_client()
    try:
        client.upload_file(filename, bucket_creds.bucket, "darwin_file")
    except Exception as e:
        logging.error(f"Exception trying to upload file: {e}")
        return False
    
    
    logging.info("Upload file passed OK")
    return True

def test_download_file(bucket_creds:credentials.Credentials,filename:str)->bool:
    logging.info("########################\nStarting test_download_file\n--------------------------")   
    client=bucket_creds.get_client()
    try:
        response=client.download_file(bucket_creds.bucket, filename, "temp"+filename)
    except Exception as e:
        logging.error(f"Exception trying to download file: {e}")    
        return False
    if response['ResponseMetadata']['HTTPStatusCode']!=200:
        logging.error(f"Bad response code : {response.status_code=} trying to download file:\n{response.text=}")
        return False
    os.remove('tempdarwin.html')
    logging.info("Download file passed OK")
    return True


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
    
    logging.debug(f"\nGet Object Response:\n{response}\n\n")

    

    
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
    
    logging.debug(f"\nGet Object Response:\n{response}\n\n")
    
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

    test_results={}


    test_results["put_object"]=test_put_object(credentials.umbra_demo,target_content)

    test_results["get_object"]=test_get_object(credentials.umbra_demo,target_md5)

    test_results["upload_file"]=test_upload_file(credentials.umbra_demo,'darwin.html')

    test_results["download_file"]=test_download_file(credentials.umbra_demo,'darwin.html')

    test_results["delete_object"]=test_delete_object(credentials.umbra_demo,'darwin.html')

    test_results["list_buckets"]=test_list_buckets(credentials.umbra_demo)
    
    test_results["list_objects"]=test_list_objects(credentials.umbra_demo)
    
    test_results["list_objects_v2"]=test_list_objects_v2(credentials.umbra_demo)

    print("Test","Passed")
    for key,value in test_results.items():
        print(f"{key} : {value}")



