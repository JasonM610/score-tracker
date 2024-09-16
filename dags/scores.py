import json, requests
import pandas as pd

from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.providers.discord.operators.discord_webhook import DiscordWebhookOperator


BASE_URL = "https://osu.ppy.sh/api/v2"
TOKEN_URL = "https://osu.ppy.sh/oauth/token"

CLIENT_ID = Variable.get("CLIENT_ID")
CLIENT_SECRET = Variable.get("CLIENT_SECRET")

WEBHOOK_URL = Variable.get("WEBHOOK_URL")

default_args = {
    "start_date": datetime(2024, 9, 16),
    "catchup":False,
    #"retries": 1,
    #"retry_delay": timedelta(minutes=5),
}

def send_request(endpoint, params=None):
    session = OAuth2Session(client_id=CLIENT_ID, token={"access_token": Variable.get("ACCESS_TOKEN")})
    return session.get(f"{BASE_URL}/{endpoint}", json=params).json()

@dag(schedule="@daily", default_args=default_args)
def taskflow():
    @task_group()
    def get_top5_scores():
        @task()
        def authenticate():
            client = BackendApplicationClient(client_id=CLIENT_ID, scope=["public"])
            session = OAuth2Session(client=client)
            
            token = session.fetch_token(
                token_url=TOKEN_URL,
                client_id=CLIENT_ID,
                client_secret=CLIENT_SECRET,
            )

            Variable.set("ACCESS_TOKEN", token["access_token"])

        @task()
        def get_users():
            endpoint = "rankings/osu/performance"
            users = []

            for page in range(1, 6):
                users_dict = send_request(endpoint, params={"page": page})
                users += [user["user"]["id"] for user in users_dict["ranking"]]

            return users
    
        @task()
        def get_recent_scores(user_ids):
            scores = []
            min_date = (datetime.now(tz=timezone.utc) - timedelta(days=1)).date()

            for user_id in user_ids:
                user_scores = send_request(f"users/{user_id}/scores/best", params={"mode": "osu", "limit": 100})

                for score in user_scores:
                    if datetime.strptime(score["created_at"], "%Y-%m-%dT%H:%M:%SZ").date() < min_date:
                        continue

                    beatmap_title = score["beatmapset"]["title"][:32] + ("..." * (len(score["beatmapset"]["title"]) > 35))
                    beatmap_version = score["beatmap"]["version"][:32] + ("..." * (len(score["beatmap"]["version"]) > 35))

                    scores.append({
                        "user_id": user_id,
                        "username": score["user"]["username"],
                        "beatmap_id": score["beatmap"]["id"],
                        "accuracy": round(score["accuracy"], 4),
                        "pp": score["pp"],
                        "mods": ("").join(score["mods"] or "NM"),
                        "grade": score["rank"],
                        "beatmap_url": score["beatmap"]["url"],
                        "artist": score["beatmapset"]["artist"],
                        "title": beatmap_title,
                        "version": beatmap_version,
                        "created_at": datetime.fromisoformat(score["created_at"].replace("Z", "+00:00")).timestamp(),
                    })

            return pd.DataFrame(scores)

        @task()
        def process_scores(scores):
            top_scores = scores.nlargest(5, 'pp')
            top_scores.to_csv("/tmp/scores.csv", index=False)

        auth = authenticate()
        users = get_users()
        scores = get_recent_scores(users)
        top5_scores = process_scores(scores)

        auth >> users >> scores >> top5_scores

    @task_group()
    def display_scores(scores):
        @task
        def build_message():
            scores = pd.read_csv("/tmp/scores.csv")

            embed = {
                "title": f"Top Plays — {((datetime.now(tz=timezone.utc)) - timedelta(days=1)).strftime("%m/%d/%Y")}",
                "color": 0xb483f6,
                "fields": [],
                "thumbnail": {
                    "url": f"https://b.ppy.sh/{scores['beatmap_id'].iloc[0]}"
                },
            }

            row_value = ""

            for i, row in scores.iterrows():
                username = f"**{row["username"]}**"
                beatmap = f"**[{row["title"]} [{row["version"]}]]({row["beatmap_url"]})**"
                mods = f"{row["mods"]}"
                pp = f"**{int(row["pp"])}pp**"
                accuracy = f"{round(row["accuracy"] * 100, 2)}%"
                grade = f"{row["grade"]}"

                row_value += f"**{i+1})** {username} — {beatmap}\n" + \
                    f" • {pp}, +{mods}\n" + \
                    f" • {accuracy} {grade} — 100x/1000x\n"
                

            embed["fields"].append(
                {
                    "name": "",
                    "value": row_value,
                }
            )
            
            payload = {
                "embeds": [embed]
            }

            return payload

        @task
        def send_message(payload):
            requests.post(
                WEBHOOK_URL, 
                headers={
                    'Content-Type': 'application/json'
                }, 
                data=json.dumps(payload)
            )

        payload = build_message()
        sent = send_message(payload)      

        payload >> sent

    scores = get_top5_scores()
    discord = display_scores(scores)

    scores >> discord

taskflow()
