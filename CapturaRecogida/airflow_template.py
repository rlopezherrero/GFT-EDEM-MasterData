import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import requests
import json
import pysftp
import  time;


def download_json_comments(json_file_arg):

	req = requests.get('https://jsonplaceholder.typicode.com/comments')
	print(req.json())

	with open('comments.json', 'w') as json_file:
		json.dump(req.json(), json_file_arg)


def upload_sftp(json_file_arg):

	cnopts = pysftp.CnOpts()
	cnopts.hostkeys = None
    
	srv = pysftp.Connection(host="35.222.158.208", username="edemExercise3",
	password="edemExercise3",cnopts = cnopts)

	with srv.cd('Upload/Roberto'): #chdir to your folder
		srv.put(json_file_arg) #upload file to it

	# Closes the connection
	srv.close()


default_args = {

    'owner': 'user', #Put your name

    'retries': 1,
	
	'start_date': datetime.datetime(2019, 9, 1),

    'retry_delay': datetime.timedelta(minutes=5),

}



with DAG('airflow_example', default_args=default_args, schedule_interval='*/3 * * * *',) as dag:

	json_file = "comments."+str(time.time())+".json"
	
	download_json_task = PythonOperator(task_id='Get_comments', python_callable=download_json_comments,json_file_arg=json_file)
	
	print_json_ok = BashOperator(task_id='Download_check', bash_command='echo "Json comments file downloaded successfully"')

	upload_json_sftp_task = PythonOperator(task_id='Upload_comments_sftp', python_callable=upload_sftp,json_file_arg=json_file)

	print_upload_ok = BashOperator(task_id='Upload_check', bash_command='echo "Json comments file uploaded successfully"')


download_json_task >> print_json_ok >> upload_json_sftp_task >> print_upload_ok