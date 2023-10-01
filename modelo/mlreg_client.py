import requests
import pickle
import pandas as pd
import boto3
import logging
import json

from io import StringIO


BASE_URL = #ENTER URL HERE
API_KEY =  #ENTER YOUR API KEY HERE

logging.getLogger().setLevel(logging.INFO)

class S3ConnectionML():

    def __init__(self, bucket=None, client_secret=None):

        self.bucket=bucket
        self.s3_client = self.get_client(client_secret)

    @staticmethod
    def get_client(client_secret =None):
        if client_secret:
            try:
                with open(client_secret) as f:
                    data = json.load(f)
            
                    ACCESS_KEY = data.get('ACCESS_KEY')
                    SECRET_KEY = data.get('SECRET_KEY')

                    s3 = boto3.client('s3',   
                                aws_access_key_id=ACCESS_KEY,
                                aws_secret_access_key=SECRET_KEY,)
                    return s3
            except:
                logging.error('Invalid crediential, s3 client not loaded!')
        
        else:
            try:
                s3 = boto3.client('s3')
                return s3
            except:
                logging.error('Could not connect to s3 with default credentials')


    def read_from_s3(self, filename, nrows=None):

        response = self.s3_client.get_object(Bucket=self.bucket, Key=filename)
        df = pd.read_csv(response.get("Body"),encoding = 'iso-8859-15', nrows=nrows)
        logging.info("The file {} has been loaded and has this shape {}".format(filename,df.shape))
        return df

    def load_from_s3(self, filename, nrows=None):

        response = self.s3_client.get_object(Bucket=self.bucket, Key=filename)
        
        
        return response.get('body')


    def df_to_s3(self, df=None, key=None):

        csv_buffer=StringIO()
        df.to_csv(csv_buffer, index=False)
        self.s3_client.put_object(Body=csv_buffer.getvalue(),
                            Bucket=self.bucket,
                            Key=key)

        logging.info(f"File with {df.shape[0]} rows was written to {key}")


    def s3_find_csv(self, path=None, suffix='csv'):

        objects = self.s3_client.list_objects_v2(Bucket=self.bucket)['Contents']

        return [obj['Key'] for obj in objects if path in obj['Key'] and suffix in obj['Key'] ]


    def s3_load_file(self,key=None) -> object:
        
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            logging.info(f"key '{self.bucket}/{key}' has been dowloaded from S3!")
            return response['Body'].read()
        except:
            logging.error(f"The key '{self.bucket}/{key}' was not downloaded make sure the file exists.")

    def s3_upload(self,file,key) -> None:
        """
        Upload an object to S3
        """
        #try:
        self.s3_client.upload_file(file, self.bucket, key)
        #except:
        #    logging.error(f"The file {file} could not be uploaded to {self.bucket}")

    def s3_download(self, file, key):

        #try:

            self.s3_client.download_file(self.bucket, key, file)
           


class Ml_registry():

    def __init__(self, user_id, company_id,bucket_name=None, bucket_folder = None, client_secret=None):

        self.user_id = user_id
        self.company_id=company_id
        self.bucket_name = bucket_name
        self.bucket_folder = bucket_folder
        self.artifact_path = None
        if bucket_name and bucket_folder:
            if client_secret:
                self.client_secret = client_secret
            else:
                self.client_secret = './secret/client_secret.json'

            self.S3 = S3ConnectionML(bucket=bucket_name,client_secret=self.client_secret)



    def create_model_version(self, model_name = None,
                            model_description = None,
                            version_name = None,
                            version_description = None,
                            parameters = None,
                            variables = None,
                            pipeline_id = None,
                            target_desc= None):

        data = {"model_name":model_name,
                "model_description": model_description,
                "version_name":version_name,
                "version_description" : version_description,
                "version_parameters" : parameters,
                "variables" : variables,
                "user_id": self.user_id,
                "company_id" : self.company_id,
                "bucket_folder" : self.bucket_folder,
                "pipeline_id" : pipeline_id,
                "target_desc" :  target_desc}



        url = f'{BASE_URL}/models/{self.company_id}'
        
        logging.info(f"Sending this data {data}")
        headers = {"api_key": API_KEY}
        response = requests.post(url,json=data, headers=headers)

        response_data = response.json().get('data')

        self.artifact_path = response_data.get('artifacts_path')

        logging.info(f"The artifacts path is '{self.artifact_path}'")

        return response_data

    def upload_model(self, filepath) -> None:
        """
        Upload a pickeled model to S3
        params:
        filepath : the path of the pickle to upload
        """

        key_name = f"{self.artifact_path}model.pkl"
        
        #try:
        logging.info(f"uploading '{filepath}' to {key_name}")
        self.S3.s3_upload(filepath,key_name)
        #except:
            #logging.error(f"Could not upload the file {filepath} to S3")
            
            # TODO DELETE THE MODEL FROM THE DATABASE


    def change_status(self,model_id:int, version_id:int, status:str) -> None:

            url = f"{BASE_URL}/versions/{model_id}"
            headers = {"api_key": API_KEY}
            
            data = { "version_id": version_id,
                     "status" : status
                    }
        
            logging.info(f"Sending this data {data}")
            response = requests.patch(url,json=data, headers=headers)
            print(response.json())
    



    def _load_model(self,model_id):
        """
        load model info from ml_registry
        """

        url=f'{BASE_URL}/versions/{model_id}'
        headers = {"api_key": API_KEY}
       
        response = requests.get(url, headers=headers)
    
        return response.json()

    def load_model_version(self, model_id:int, version_id:int = None,load_pickle :bool =True,filename:str = None, status:str = None) :
        """
        This function will download a model from the model registry.
        To do this we need to 
        1. Find the model path
        2.unpickle it
        3. return the (model, json (version description)
        """

        data = self._load_model(model_id).get('data')
        print(data)

        
        for m in data: 
            if version_id:
                if m.get('version') == version_id:
                    logging.info(f"downloading version {version_id} of model_id {model_id}")
                    _path = m.get('artifacts_path')
                    version_data = m
                    key_name = f"{_path}{filename}"

            elif status:
                if m.get('status') == status:
                    logging.info(f"downloading version in PROD of model_id {model_id}")
                    _path = m.get('artifacts_path')
                    version_data = m
                    key_name = f"{_path}{filename}"

            else: 
                logging.info(f"downloading latest version of model_id {model_id}")
                _path = m.get('artifacts_path')
                version_data = m
                key_name = f"{_path}{filename}"

        if load_pickle:      
            logging.info('Downloading ')
            if _path[0] == '/':
                _path = _path[1:]

                key_name = f"{_path}{filename}"
            print(f'key_name is {key_name}')
            model = pickle.loads(self.S3.s3_load_file(key_name))
            return model, version_data

        else:
            return version_data


    def get_model_by_name(self, model_name):

        url = f"{BASE_URL}/model/name/{model_name}"
        headers = {"api_key": API_KEY}

        response = requests.get(url,headers=headers)

        return response.json()


    



    def get_model_info(self, model_name):

        url = f"{BASE_URL}/model/name/{model_name}"
        headers = {"api_key": API_KEY}

        response = requests.get(url,json=data, headers=headers)

        model_id = response.get('id')

        prod_version = self.track_version()



    def track_version(self, version_id:int, validation_dt:str, stat_name:str, data:str) ->str:
        """
        Add a tracking record to a given version_id.
        Params
        --------------------------
        version_id : Int. The version id that is been tracked
        validation_dt ;  Str :  The date of the data that was used for track
        stat_name : Str, The stat that is log eg. AUC, LIFT, LOSS, etc
        data : Str, The value of the stat_name.
        """

   
        url = f'{BASE_URL}/validation/{version_id}'
    
        data = {"user_id": self.user_id,
                "validation_dt" : validation_dt,
                "stat_name": stat_name.upper(),
                "data" : data}

        headers = {"api_key": API_KEY}
        try:
            logging.info(f"Sending this data {data}")
            response = requests.post(url,json=data, headers=headers)

            return response.json()
        except:
            logging.error("Couldn't record Track row in ML Registry for version_id {version_id}")


if __name__ == "__main__":



        data = {"model_name":'demo23',
            "model_description": 'Useful description',
            "version_name":'v1',
            "version_description" : 'random forest',
            "pipeline_id" : 3,
            "parameters" : "'type': 'RandomForest','n_leafs':25,'depth':3",
            "variables": "['a','b','c']"
          }
        
        ml_r = Ml_registry(company_id=1, user_id=1,bucket_name='modelfactorymarcos',bucket_folder = 'ml_registry')
        r = ml_r.create_model_version(**data)
        #print(r)
        #filename = 'mypick.pkl'
        #ml_r.upload_model(filename)
        #print(r)
        #print('*'*20)


        #print(ml_r.load_model(5))
        #ml_r.change_status(6,5,'PROD')
        #ml_r = Ml_registry(company_id=1, user_id=1)
        #model_name = 'Demo_pipeline'
        #model_id = ml_r.get_model_by_name(model_name).get('data').get('id')
        #print(f'model_id id {model_id}')
        #m_data = ml_r.load_model_version(model_id,status='PROD', load_pickle=False)
        
        #f_data = {'model_full_name' : f"{model_name} - v{m_data.get('version')}",
        #            "variables" : m_data.get('variables')}

        #print(f_data)


        ml_r = Ml_registry(company_id=1, user_id=1,bucket_name='modelfactorymarcos',bucket_folder = 'ml_registry')
        modelo, desc = ml_r.load_model_version(8,status='PROD', filename='model.pkl')

         
