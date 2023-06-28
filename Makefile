include .env

ec2-server:
	ssh -i ${AIRFLOW_SERVER_ACCESS_KEY} ${AIRFLOW_SERVER}