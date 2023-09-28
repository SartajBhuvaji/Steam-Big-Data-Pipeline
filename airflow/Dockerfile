FROM apache/airflow:2.2.3

# Set the desired Python version (Python 3.9 in this case)

COPY requirements.txt .
RUN pip install -r requirements.txt

RUN pip install markupsafe==2.0.1 \
	&& pip install apache-airflow-providers-odbc \
	&& pip install pyodbc \
	&& pip install apache-airflow-providers-microsoft-mssql \
	&& pip install apache-airflow-providers-microsoft-mssql[odbc] \
	&& pip install apache-airflow-providers-microsoft-azure \
	&& pip install gitpython 

RUN pip install selenium==4.11.2 

RUN pip install kafka_python==2.0.2
	#docker build --no-cache -t docimg . docker logs airflow

