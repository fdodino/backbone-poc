from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pandas import DataFrame, read_csv, read_json, read_sql_query
from airflow.providers.postgres.hooks.postgres import PostgresHook


# This version fixes some of the previous anti-patterns
# 1. we query data in Postgres using Airflow connections
#
# TODO
# 2. writing in local file
# 3. there are some top level functions that could be inner functions since
#    we don't need to reuse them
# 4. using now() function is not bad since they allow us to define the name
#    of the file
#
# Good practice: replacing the values in the table avoids duplicate records
#
default_args = {
    "owner": "fdodino",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "connection_id": "backbone"
}


postgres_hook = PostgresHook(default_args["connection_id"])
connection = postgres_hook.get_sqlalchemy_engine()


# constants
EMPLOYEE_INPUT_CSV_FILE = r"./dags/data/employees.csv"
EMPLOYEES_OUTPUT_CSV_FILE = r"./dags/results/special_employees.csv"


# In this example we
# - take an employee csv file as input
# - we filter those employees born before 1950
# - and create a json file using some of the fields
# - the JSON file
@dag(
    dag_id="10_employee_data_best_practice",
    description="DAG for employees using best practices - PoC",
    start_date=datetime(2022, 3, 1, 2),
    schedule_interval="@daily",
)
def employees():
    @task
    def data_validation(ti=None):
        def now():
            return datetime.now().strftime(r"%Y%m%d%H%M%S")

        # internal transformation function
        def year_of_birth(date_of_birth):
            return datetime.strptime(
                date_of_birth, "%Y-%m-%d"
            ).year

        # validation hook function
        # we create a calculated column for the data frame (in memory),
        # in order to create a pandas filter
        def filter_employees_by_year_of_birth(employees):
            employees["YearOfBirth"] = employees["DateOfBirth"] \
                .apply(year_of_birth)
            return employees[employees["YearOfBirth"] < 1950]

        # data_validation_body
        original_employees = read_csv(EMPLOYEE_INPUT_CSV_FILE)
        oldies_file = f"./dags/results/oldies_{now()}.json"
        filter_employees_by_year_of_birth(original_employees) \
            .filter(items=["UserId", "FirstName", "LastName"]) \
            .to_json(oldies_file, orient="records")
        return oldies_file

    @task
    def insert_employees(ti=None):
        oldies_file = ti.xcom_pull(task_ids="data_validation")
        original_employees = read_json(oldies_file).rename(columns={
            "UserId": "id",
            "FirstName": "first_name",
            "LastName": "last_name"
        })
        original_employees \
            .to_sql("employees", con=connection,
                    if_exists="replace", index=False)

    @task
    def read_employees():
        query_employees = read_sql_query('''
            SELECT id, first_name, last_name FROM "employees";
        ''', connection)
        DataFrame(query_employees).to_csv(EMPLOYEES_OUTPUT_CSV_FILE)

    data_validation() >> insert_employees() >> read_employees()


employees()
