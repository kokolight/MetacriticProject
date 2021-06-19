import requests
import bs4
from bs4 import BeautifulSoup
import pandas as pd
import threading
import re
import os.path
from os import path

# Retrieving Data from metacritic "https://metacritic.com" (Main) some other details from API "https://rawg.io"

# Data we scrape
"""
- Name
- Release Date
- Metascore
- Userscore
"""
# Data we get from API
"""
- Singleplayer - True/False
- Multiplayer - True/False
- Achievements count
- Youtube related videos count
- Average playtime
- Is part of a series
- Publisher
- Developer
- Number of consoles
- Maturaty Rating
"""


def get_game_rawg_details(game_name):
    RAWG_URL = f"https://api.rawg.io/api/games/{game_name}?key={RAWG_API_KEY}"
    data = requests.get(RAWG_URL, headers=HEADERS).json()
    if "redirect" in data:
        game_name = data["slug"]
        RAWG_URL = f"https://api.rawg.io/api/games/{game_name}?key={RAWG_API_KEY}"
        data = requests.get(RAWG_URL, headers=HEADERS).json()
    elif "detail" in data:
        return []

    game_details = {
        "achievements_count": data["parent_achievements_count"],
        "avg_playtime": data["playtime"],
        "youtube_videos_count": data["youtube_count"],
        "singleplayer": False,
        "multiplayer": False,
        "game_series_count": data["game_series_count"],
        "maturaty_rating": data["esrb_rating"]["name"][0] if data["esrb_rating"] else None,
        "platforms_count": len(data["platforms"]),
        "publisher": data["publishers"][0]["name"] if len(data["publishers"]) else None,
        "developer": data["developers"][0]["name"] if len(data["developers"]) else None,
        "ratings_count": data["ratings_count"],
    }
    for tag in data["tags"]:
        if tag["slug"] == "singleplayer":
            game_details["singleplayer"] = True
        if tag["slug"] == "multiplayer":
            game_details["multiplayer"] = False
    return game_details


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"
}
RAWG_API_KEY = "1a30728a7348454fa81a4a98e057442d"
FILE_NAME = "video_games_ratings.csv"
BASE_URL = "https://www.metacritic.com"
BROWSE_URL = BASE_URL + "/browse/games/score/metascore/all/all/filtered?"
COLUMNS = [
    "game_name",
    "metascore",
    "userscore",
    "platforms_count",
    "awards_count",
    "publisher",
    "developer",
    "release_date",
    "maturaty_rating",
    "singleplayer",
    "multiplayer",
    "youtube_videos_count",
    "achievements_count",
    "ratings_count",
    "average_playtime",
    "game_series_count",
]

month_dict = {
    "January": "1",
    "February": "2",
    "March": "3",
    "April": "4",
    "May": "5",
    "June": "6",
    "July": "7",
    "August": "8",
    "September": "9",
    "October": "10",
    "November": "11",
    "December": "12",
}


def get_game_data(game_slug, game_name, metascore, userscore, release_date):
    rawg_details = get_game_rawg_details(game_slug)
    if len(rawg_details) == 0:
        return []

    # meta_score = soup.find('div', class_="metascore_w").text
    return {
        "game_name": game_name,
        "release_date": release_date,
        "metascore": metascore,
        "userscore": userscore,
        "publisher": rawg_details["publisher"],
        "developer": rawg_details["developer"],
        "maturaty_rating": rawg_details["maturaty_rating"],
        "platforms_count": rawg_details["platforms_count"],
        "average_playtime": rawg_details["avg_playtime"],
        "achievements_count": rawg_details["achievements_count"],
        "youtube_videos_count": rawg_details["youtube_videos_count"],
        "singleplayer": rawg_details["singleplayer"],
        "multiplayer": rawg_details["multiplayer"],
        "ratings_count": rawg_details["ratings_count"],
        "game_series_count": rawg_details["game_series_count"],
    }


def metacritic_games_data(start_page, end_page):
    index = 1
    game_records = []

    current_data = pd.read_csv(FILE_NAME, index_col=0) if path.exists(FILE_NAME) else pd.DataFrame(columns=COLUMNS)
    soup = BeautifulSoup(features="lxml")
    # Get all games in all pages
    for i in range(start_page, end_page):
        page_url = BROWSE_URL + "page={0}".format(i)
        req = requests.get(page_url, headers=HEADERS)
        soup.append(bs4.BeautifulSoup(req.text, "lxml"))

    trs = set(soup.select("tr"))
    spacers = set(soup.select("tr.spacer"))
    games = trs - spacers

    # For each game
    for game in games:
        game_name = game.select("h3")[0].text
        # Already exists
        if (not current_data.empty) and (game_name in current_data["game_name"].values):
            print(f"Already in dataframe - {game_name}")
            index = index + 1
            continue
        metascore = game.select("div.metascore_w.game")[0].text
        userscore = game.select("div.metascore_w.user.game")[0].text
        date = game.select("div.clamp-details")[0].find_all("span")[2].text.replace(",", "")
        date_splitted = date.split(" ")
        release_date = "{day}-{month}-{year}".format(
            day=date_splitted[1], month=month_dict[date_splitted[0]], year=date_splitted[2]
        )
        game_href = game.find("a", class_="title", href=True)["href"]
        game_slug = re.findall(".*\/(.*)$", game_href)[0]

        print(f"Game: {game_name}")

        game_object = get_game_data(game_slug, game_name, metascore, userscore, release_date)
        if len(game_object):
            game_records.append(game_object)
        else:
            print(f"Couldn't get game details: {game_name}")

        print("Progress: {:.2f}%".format((index / len(games)) * 100))
        index = index + 1

    new_data = pd.DataFrame(game_records)

    if not new_data.empty:
        current_data = current_data.append(new_data, ignore_index=True)
        current_data.to_csv("./video_games_ratings.csv", mode="w")
    return current_data


df = metacritic_games_data(1, 30)
print(df)
