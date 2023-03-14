AIRFLOW_HOME = ~/Desktop/astro

deploy_traditional:
	cp dags/traditional/*.py $(AIRFLOW_HOME)/dags

deploy_task_flow:
	cp dags/task_flow/*.py $(AIRFLOW_HOME)/dags

deploy_mysql:
	cp dags/mysql/*.py $(AIRFLOW_HOME)/dags
