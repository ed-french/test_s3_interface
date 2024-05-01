import logging
if __name__=="__main__":
    logging.basicConfig(level=logging.INFO)

import boto3
import credentials
import hashlib
import os
import threading
import credential_manager as cm

import botocore
import time

from concurrent.futures import ThreadPoolExecutor,as_completed

client_config = botocore.config.Config(
    max_pool_connections=1000,
)




def test_list_objects(bucket_creds:cm.Credentials)->bool:
    logging.info("########################\nStarting test_list_objects\n--------------------------")
    client=get_client(bucket_creds)
    try:
        response=client.list_objects(Bucket=bucket_creds.bucket)
    except Exception as e:
        logging.error(f"Exception trying to list objects: {e}")
        return False
    
    if response['ResponseMetadata']['HTTPStatusCode']!=200:
        logging.error(f"Bad response code : {response.status_code=} trying to list objects:\n{response.text=}")     
        return False

def test_list_objects_v2(bucket_creds:cm.Credentials)->bool:
    logging.info("########################\nStarting test_list_objects_v2\n--------------------------")
    client=get_client(bucket_creds)
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


def test_delete_object(bucket_creds:cm.Credentials,key:str)->bool:
    logging.info("########################\nStarting test_delete_object\n--------------------------")
    client=get_client(bucket_creds)
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

def get_client(bucket_creds:cm.Credentials)->boto3.client:
    return bucket_creds.get_client(config=client_config)

def test_list_buckets(bucket_creds:cm.Credentials)->bool:
    logging.info("########################\nStarting test_list_buckets\n--------------------------")    
    client=get_client(bucket_creds)
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


def test_upload_file(bucket_creds:cm.Credentials,filename:str)->bool:
    logging.info("########################\nStarting test_upload_file\n--------------------------")
    client=get_client(bucket_creds)
    try:
        client.upload_file(filename, bucket_creds.bucket, "darwin_file")
    except Exception as e:
        logging.error(f"Exception trying to upload file: {e}")
        return False
    
    
    logging.info("Upload file passed OK")
    return True

def test_download_file(bucket_creds:cm.Credentials,filename:str)->bool:
    logging.info("########################\nStarting test_download_file\n--------------------------")   
    client=get_client(bucket_creds)
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


def test_put_object(bucket_creds:cm.Credentials,test_data:bytearray)->bool:
    logging.info("########################\nStarting test_put_object\n--------------------------")

    client=get_client(bucket_creds)
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

def test_get_object(bucket_creds:cm.Credentials,target_md5)->bool:
    logging.info("########################\nStarting test_get_object\n--------------------------")
    client=get_client(bucket_creds)
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

def spam_write_one(client:boto3.client,bucket:str,payload:bytearray,name:str,results:dict[str,bool]):
    """
    Returns an entry in results against
    each test "name" which will be bool to indicate if that test works
    """


    try:
        response=client.put_object(Body=payload,
                            Bucket=bucket,
                            Key=name)
    except Exception as e:
        results[name]=False
        logging.error(f"Exception trying to put object: {e}")
        return

    if response['ResponseMetadata']['HTTPStatusCode']!=200:
        results[name]=False
        return
    
    results[name]=True
    return


def test_spam_write(bucket_creds:cm.Credentials,sample_count=10)->bool:
    logging.info("########################\nStarting test_spam_write\n--------------------------")

    client=get_client(bucket_creds)

    payload=b'this is a small payload'

    results={}
    threadset=[]
    for i in range(sample_count):
        name=f"test{i}"
        threadset.append(threading.Thread(target=spam_write_one,
                                          args=(client,bucket_creds.bucket,payload,name,results)))
    
    # Start them all
    for thr in threadset:
        thr.start()
    
    #Wait for them all to finish
    for thr in threadset:
        thr.join()



    overall_result=all(results.values())
    logging.info("Spam write result:"+("PASS" if overall_result else "FAIL"))

    if not overall_result:
        logging.info(f"Spam write details:"+",".join(["1" if x else "0" for x in results.values()]))

    return overall_result

def spam_read_one(client:boto3.client,bucket:str,name:str)->bool:
    """
    Returns an entry in results against
    each test "name" which will be bool to indicate if that test works
    
    """
    try:
        response=client.get_object(Bucket=bucket, Key=name)
    except Exception as e:
        logging.error(f"Exception trying to get object: {e}")
        return False
    
    if response['ResponseMetadata']['HTTPStatusCode']!=200:
        print("~",end="")
        return False
    


    return True


   
def test_spam_read(bucket_creds:cm.Credentials,sample_count=10)->bool:
    logging.info("########################\nStarting test_spam_read\n--------------------------")

    client=get_client(bucket_creds)

    results=[]
    futures=[]
    with ThreadPoolExecutor() as executor:
        
        for i in range(sample_count):
            name=f"test{i}"
            print(">",end="")
            futures.append(
                executor.submit(spam_read_one, 
                                client, 
                                bucket_creds.bucket, 
                                name))
    
    print("\nDone creating threads")
    for future in as_completed(futures):
        results.append(future.result())

    overall_result=all(results)
    logging.info("Spam read result:"+("PASS" if overall_result else "FAIL"))

    if not overall_result:
        logging.info(f"Spam read details:"+",".join(["1" if x else "0" for x in results])) 

    return overall_result


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

    test_results["spam_write_results"]=test_spam_write(credentials.umbra_demo,500)

    test_results["spam_read_results"]=test_spam_read(credentials.umbra_demo,500)

    print(f"{'Test':<20} : Result?")
    for key,value in test_results.items():
        print(f"{key:<20} : {value}")



