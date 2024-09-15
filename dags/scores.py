from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient
from datetime import datetime, timedelta, timezone
import json
import pandas as pd


BASE_URL = "https://osu.ppy.sh/api/v2"
TOKEN_URL = "https://osu.ppy.sh/oauth/token"

CLIENT_ID = Variable.get("CLIENT_ID")
CLIENT_SECRET = Variable.get("CLIENT_SECRET")

default_args = {
    "start_date": datetime(2024, 9, 5),
    "catchup":False,
    #"retries": 1,
    #"retry_delay": timedelta(minutes=5),
}

@dag(schedule="@daily", default_args=default_args)
def taskflow():
    @task()
    def get_users():
        users_path = "/opt/airflow/dags/data/users.json"
        with open(users_path, "r") as file:
            users = json.load(file)
            return users

    @task_group()
    def get_top5_scores(users):
        @task()
        def get_recent_scores(users):
            scores = []
            min_date = (datetime.now(tz=timezone.utc) - timedelta(days=6)).date()


            client = BackendApplicationClient(client_id=CLIENT_ID, scope=["public"])
            session = OAuth2Session(client=client)
            session.fetch_token(
                token_url=TOKEN_URL,
                client_id=CLIENT_ID,
                client_secret=CLIENT_SECRET,
            )
        
            for user in users:
                for mode in user["modes"]:
                    user_scores = session.request(
                        "GET",
                        f"{BASE_URL}/users/{user["osu_user_id"]}/scores/best",
                        json={
                            "mode": mode,
                            "limit": 100,
                        },
                    ).json()

                    for score in user_scores:
                        score_date = datetime.strptime(score["created_at"], "%Y-%m-%dT%H:%M:%SZ").date()
                        if score_date >= min_date:
                            scores.append({
                                "discord_id": user["discord_user_id"],
                                "osu_id": user["osu_user_id"],
                                "username": score["user"]["username"],
                                "beatmap_id": score["beatmap"]["id"],
                                "accuracy": round(score["accuracy"], 4),
                                "pp": score["pp"],
                                "mods": ("").join(score["mods"]),
                                "grade": score["rank"],
                                "beatmap_url": score["beatmap"]["url"],
                                "artist": score["beatmapset"]["artist"],
                                "title": score["beatmapset"]["title"],
                                "version": score["beatmap"]["version"],
                                "created_at": score["created_at"],
                            })

            return pd.DataFrame(scores)

        @task()
        def process_scores(scores):
            scores.sort_values(by="pp", ascending=False)

            scores.to_csv("/tmp/scores.csv", index=False)

        scores = get_recent_scores(users)
        top5_scores = process_scores(scores)

    @task_group()
    def display_scores(scores):
        pass

    users = get_users()
    scores = get_top5_scores(users)
    discord = display_scores(scores)

    users >> scores >> discord


taskflow()
