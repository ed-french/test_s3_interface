from dataclasses import dataclass
import boto3

@dataclass
class Credentials:
  """
  Holds set of credentials for either Umbra or AWS
  """
  region_name: str
  endpoint_url: str
  aws_access_key_id: str
  aws_secret_access_key: str
  forcePathStyle: bool
  bucket: str






  

  def get_client_creds(self):
    """
    returns a dictionary that can be unpacked to create a boto3 client
    """
    res={}
    res["region_name"]=self.region_name
    res["endpoint_url"]=self.endpoint_url
    res["aws_access_key_id"]=self.aws_access_key_id
    res["aws_secret_access_key"]=self.aws_secret_access_key

    return res
  
  def get_client(self,config:dict|None=None)->boto3.client:
    """
    returns a boto3 client
    """
    return boto3.client("s3",**self.get_client_creds(),config=config)