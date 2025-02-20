# Airflow setup

1. Download the official Docker-compose YAML file for the latest Airflow version
```
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.2.3/docker-compose.yaml'
```
- [The official docker-compose.yaml file is quite complex and contains several service definitions.](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#docker-compose-yaml)
- For a refresher on how docker-compose works, you can check out this lesson from the ML Zoomcamp.

2. We now need to set up the Airflow user. For MacOS, create a new .env in the same folder as the docker-compose.yaml file with the content below:
```
AIRFLOW_UID=50000
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

3. The base Airflow Docker image won't work with GCP, so we need to [customize](https://airflow.apache.org/docs/docker-stack/build.html) it to suit our needs. You may download a GCP-ready Airflow Dockerfile [from this link](https://github.com/ziritrion/dataeng-zoomcamp/blob/main/2_data_ingestion/airflow/Dockerfile). A few things of note:
We use the base Apache Airflow image as the base.
We install the GCP SDK CLI tool so that Airflow can communicate with our GCP project.
We also need to provide a [requirements.txt](https://github.com/ziritrion/dataeng-zoomcamp/blob/main/2_data_ingestion/airflow/requirements.txt) file to install Python dependencies. The dependencies are:
apache-airflow-providers-google so that Airflow can use the GCP SDK.
pyarrow , a library to work with parquet files.

5. Alter the x-airflow-common service definition inside the docker-compose.yaml file as follows:
We need to point to our custom Docker image. At the beginning, comment or delete the image field and uncomment the build line, or arternatively, use the following (make sure you respect YAML indentation):
  build:
    context: .
    dockerfile: ./Dockerfile
Add a volume and point it to the folder where you stored the credentials json file. Assuming you complied with the pre-requisites and moved and renamed your credentials, add the following line after all the other volumes:
- ~/.google/credentials/:/.google/credentials:ro
Add 2 new environment variables right after the others: GOOGLE_APPLICATION_CREDENTIALS and AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT:
GOOGLE_APPLICATION_CREDENTIALS: /.google/credentials/google_credentials.json
AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT: 'google-cloud-platform://?extra__google_cloud_platform__key_path=/.google/credentials/google_credentials.json'
Add 2 new additional environment variables for your GCP project ID and the GCP bucket that Terraform should have created in the previous lesson. You can find this info in your GCP project's dashboard.
GCP_PROJECT_ID: '<your_gcp_project_id>'
GCP_GCS_BUCKET: '<your_bucket_id>'
Change the AIRFLOW__CORE__LOAD_EXAMPLES value to 'false'. This will prevent Airflow from populating its interface with DAG examples.

6. You may find a modified docker-compose.yaml file in this link.

7. Additional notes:
The YAML file uses CeleryExecutor as its executor type, which means that tasks will be pushed to workers (external Docker containers) rather than running them locally (as regular processes). You can change this setting by modifying the AIRFLOW__CORE__EXECUTOR environment variable under the x-airflow-common environment definition.
You may now skip to the Execution section to deploy Airflow, or continue reading to modify your docker-compose.yaml file further for a less resource-intensive Airflow deployment.


# Execution

1. Build the image. It may take several minutes You only need to do this the first time you run Airflow or if you modified the Dockerfile or the requirements.txt file.
```
docker-compose build
```

2. Initialize configs:
```
docker-compose up airflow-init
```

3. Run Airflow
```
docker-compose up airflow-init
```

4. You may now access the Airflow GUI by browsing to localhost:8080. Username and password are both airflow .


# Creating a DAG
 For reference, check out [Airflow's docs](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html).

 A DAG is created as a Python script which imports a series of libraries from Airflow.

There are [3 different ways of declaring a DAG](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#declaring-a-dag). Here's an example definition using a context manager:

```
with DAG(dag_id="my_dag_name") as dag:
    op1 = DummyOperator(task_id="task1")
    op2 = DummyOperator(task_id="task2")
    op1 >> op2
```

- When declaring a DAG we must provide at least a dag_id parameter. There are many additional parameters available.
- The content of the DAG is composed of tasks. This example contains 2 operators, which are predefined tasks provided by Airflow's libraries and plugins.
- An operator only has to be declared with any parameters that it may require. There is no need to define anything inside them.
- All operators must have at least a task_id parameter.
- Finally, at the end of the definition we define the task dependencies, which is what ties the tasks together and defines the actual structure of the DAG.
    - Task dependencies are primarily defined with the >> (downstream) and << (upstream) control flow operators.
    - Additional functions are available for more complex control flow definitions.
- A single Python script may contain multiple DAGs.

Many operators inside a DAG may have common arguments with the same values (such as start_date). We can define a default_args dict which all tasks within the DAG will inherit:

```
default_args = {
    'start_date': datetime(2016, 1, 1),
    'owner': 'airflow'
}

with DAG('my_dag', default_args=default_args) as dag:
    op = DummyOperator(task_id='dummy')
    print(op.owner)  # "airflow"
```

For this lesson we will focus mostly on operator tasks. Here are some examples:

```
download_dataset_task = BashOperator(
    task_id="download_dataset_task",
    bash_command=f"curl -sS {dataset_url} > {path_to_local_home}/{dataset_file}"
)
```

- A BashOperator is a simple bash command which is passed on the bash_command parameter. In this example, we're doenloading some file.

```
  format_to_parquet_task = PythonOperator(
      task_id="format_to_parquet_task",
      python_callable=format_to_parquet,
      op_kwargs={
          "src_file": f"{path_to_local_home}/{dataset_file}",
      },
  )
```

- A PythonOperator calls a Python method rather than a bash command.

- In this example, the python_callable argument receives a function that we've defined before in the DAG file, which receives a file name as a parameter then opens that file and saves it in parquet format.

- the op_kwargs parameter is a dict with all necessary parameters for the function we're calling. This example contains a single argument with a file name.

A list of operators is available on [Airflow's Operators docs](https://airflow.apache.org/docs/apache-airflow/stable/concepts/operators.html). A list of GCP-specific operators is also available.

As mentioned before, DAGs can be scheduled. We can define a schedule in the DAG definition by including the start_date and schedule_interval parameters, like this:

```
from datetime import datetime

with DAG(
    dag_id="my_dag_name",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2021, 1, 1)
  ) as dag:
    op1 = DummyOperator(task_id="task1")
    op2 = DummyOperator(task_id="task2")
    op1 >> op2
```

- The scheduler will run a job AFTER the start date, at the END of the interval

- The interval is defined as a [cron job expression](https://www.wikiwand.com/en/Cron[). You can use [crontab guru](https://www.wikiwand.com/en/Cron) to define your own.

- In this example, 0 6 2 * * means that the job will run at 6:00 AM on the second day of the month each month.

- The starting date is what it sounds like: defines when the jobs will start.

- The jobs will start AFTER the start date. 
    - In this example, the starting date is January 1st, which means that the job won't run until January 2nd.

- Airflow will re-scan the python files in /dags folder to refresh the DAG graphes. manually re-generate the DAG during develop can refresh the airflow UI or restart the scheduler on the command line `docker-compose restart airflow-scheduler & airflow dags reserialize`

- check the scheduler logs for errors
```
docker-compose logs airflow-scheduler

# If issues persist, force a DAG re-parse by deleting the DAG from the UI or using the CLI:
docker-compose logs airflow-scheduler
```

- access the airflow container and check the dag
```
docker-compose exec airflow-worker bash

airflow tasks test <dag_id> <task_id> <execution_date>
```