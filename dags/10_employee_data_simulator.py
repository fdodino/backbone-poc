from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pandas import DataFrame, read_csv, read_json, read_sql_query
import sqlalchemy
from airflow.models import Variable

default_args = {
    "owner": "fdodino",
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}

# constants
EMPLOYEE_INPUT_CSV_FILE = r"./dags/data/employees.csv"
EMPLOYEES_OUTPUT_CSV_FILE = r"./dags/results/special_employees.csv"

# establish connection to PostgreSQL
connection_string = Variable.get("POSTGRES_CONNECTION")
engine = sqlalchemy.create_engine(connection_string)


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


# In this example we
# - take an employee csv file as input
# - we filter those employees born before 1950
# - and create a json file using some of the fields
# - the JSON file
@dag(
    dag_id="10_employee_data_simulator",
    description="DAG for employees - PoC",
    start_date=datetime(2022, 3, 1, 2),
    schedule_interval="@daily",
)
def employees():
    @task
    def data_validation(ti=None):
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
            .to_sql("employees", con=engine,
                    if_exists="replace", index=False)

    @task
    def read_employees():
        query_employees = read_sql_query('''
            SELECT id, first_name, last_name FROM "employees";
        ''', engine)
        DataFrame(query_employees).to_csv(EMPLOYEES_OUTPUT_CSV_FILE)

    data_validation() >> insert_employees() >> read_employees()


employees()
