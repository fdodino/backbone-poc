from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pandas import read_csv

default_args = {
    "owner": "fdodino",
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}


def now():
    return datetime.now().strftime(r'%Y%m%d%H%M%S')


# internal transformation function
def year_of_birth(date_of_birth):
    return datetime.strptime(
        date_of_birth, '%Y-%m-%d'
    ).year


# validation hook function
def filter_employees_by_year_of_birth(local_employees):
    local_employees['YearOfBirth'] = local_employees['DateOfBirth'] \
        .apply(year_of_birth)
    return local_employees[local_employees['YearOfBirth'] < 1950]


@dag(
    dag_id="10_employee_data_simulator",
    description="DAG for employees - PoC",
    start_date=datetime(2022, 3, 1, 2),
    schedule_interval="@daily",
)
def employees():
    @task
    def data_validation():
        file = r"./dags/data/employees.csv"
        local_employees = read_csv(file)
        filter_employees_by_year_of_birth(local_employees) \
            .filter(items=["UserId", "FirstName", "LastName"]) \
            .to_json(f'./dags/results/oldies_{now()}.json', orient='records')

    data_validation()


employees()
