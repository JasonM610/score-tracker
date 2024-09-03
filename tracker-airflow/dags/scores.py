from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from datetime import datetime, timedelta
import json

default_args = {
    "start_date": datetime(2024, 8, 21),
    "catchup": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# create table -> extract user id and modes from table -> make api call for each one -> transform the dataframe -> create discord msg


@dag(schedule="@daily", default_args=default_args)
def taskflow():
    @task_group()
    def get_user_scores():
        @task()
        def get_users():
            users_path = "/opt/airflow/data/users.json"
            with open(users_path, "r") as file:
                users = json.load(file)
                return users

        @task()
        def get_scores(data):
            pass

        users = get_users()
        get_scores(users)

    get_user_scores = get_user_scores()


taskflow()
