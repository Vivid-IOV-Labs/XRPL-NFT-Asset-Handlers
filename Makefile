include .env

ec2-server:
	ssh -i ${AIRFLOW_SERVER_ACCESS_KEY} ${AIRFLOW_SERVER}

deploy-dev:
	mv psycopg2-2 psycopg2
	sls deploy --stage dev
	mv psycopg2 psycopg2-2