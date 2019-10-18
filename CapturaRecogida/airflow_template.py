from datetime import datetime
from datetime import timedelta
import airflow
import requests
import json
import pysftp
import  time;

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


default_args = {

	#1. Put your name
    'owner': 'Roberto', 
    'retries': 1,
	'start_date':  datetime.now(),
    'retry_delay': timedelta(minutes=5),
}

def download_json_comments():

	#2. Get comments from jsonplaceholder.typicode.com using a REST Call
	
	print(req.json())

	#3. Add your name as a Sufix of file name 
	with open("comments.json", 'w') as json_file:
		json.dump(req.json(), json_file)		
		
	print("Successfully writted on comments.json")
	


def upload_sftp():

	cnopts = pysftp.CnOpts()
	cnopts.hostkeys = None
    
	#4. Create a pysftp connection with host="35.222.158.208", username="edemExercise3", password="edemExercise3",cnopts = cnopts
	
	json_remote_file = "comments."+str(time.time())+".json"
	
	#5. Do a sftp with following parameters
	# Local file (commentsYourName.json)
	# Remote file path "Upload/YourName/json_remote_file"

	# Closes the connection
	srv.close()




#6. Add your name to the DAG name below
with DAG('airflow_sftp_YourName', default_args=default_args, schedule_interval='*/3 * * * *',) as dag:

	
	download_json_task = PythonOperator(task_id='Get_comments', python_callable=download_json_comments)
	
	print_json_ok = BashOperator(task_id='Download_check', bash_command='echo "Json comments file downloaded successfully"')

	upload_json_sftp_task = PythonOperator(task_id='Upload_comments_sftp', python_callable=upload_sftp)
	
	print_upload_ok = BashOperator(task_id='Upload_check', bash_command='echo "Json comments file uploaded successfully"')


#7. Create worflow DAG with following sequence: download_json_task, print_json_ok, upload_json_sftp_task, print_upload_ok
