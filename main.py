import requests
import bs4
from bs4 import BeautifulSoup
import pandas as pd
import threading
import re
import os.path
from os import path

file_name = "video_games_ratings.csv"
base_url = "https://www.metacritic.com"
browse_url = base_url + '/browse/games/score/metascore/all/all/filtered?'
columns = ['name', 'metascore', 'userscore', 'platform', 'platforms_count', 'awards_count', 'publisher', 'developer', 'release_date',
           'mature_rating', 'positive_critic_reviews_count', 'mixed_critic_reviews_count', 'negative_critic_reviews_count',
           'positive_user_reviews_count', 'mixed_user_reviews_count', 'negative_user_reviews_count']

month_dict = {'January': '1', 'February': '2', 'March': '3', 'April': '4', 'May': '5', 'June': '6', 'July': '7',
              'August': '8', 'September': '9', 'October': '10', 'November': '11', 'December': '12'}

soup = BeautifulSoup('html.parser')


def metacritic_games_data(start_page, end_page):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'}
    index = 1
    game_records = []

    current_data = pd.read_csv(file_name, index_col=0) if path.exists(
        file_name) else pd.DataFrame(columns=columns)

    # Get all games in all pages
    for i in range(start_page, end_page):
        page_url = browse_url+'page={0}'.format(i)
        req = requests.get(page_url, headers=headers)
        soup.append(bs4.BeautifulSoup(req.text))

    games_objects = soup.find_all("td", class_="clamp-summary-wrap")

    # For each game
    for game in games_objects:
        record = {}
        title_obj = game.find('a', class_="title")
        title = title_obj.text

        # Already exists
        if (not current_data.empty) and (title in current_data['name'].values):
            print(f"Already in dataframe - {title}")
            continue

        href = title_obj.get("href")
        meta_score = game.find('div', class_="metascore_w").text
        platform = game.find("span", class_="data").text.strip()
        date = game.find(
            "div", class_="clamp-details").find_all("span")[2].text.replace(",", "")
        date_splitted = date.split(" ")
        release_date = "{day}-{month}-{year}".format(
            day=date_splitted[1], month=month_dict[date_splitted[0]], year=date_splitted[2])

        # add the values
        record.update({'name': title, 'metascore': meta_score,
                       'platform': platform, 'release_date': release_date})

        # in game section
        game_url = base_url+''+href
        game_req = requests.get(game_url, headers=headers)
        print("Progress: {:.2f}%".format(
            (index/100*(end_page-start_page))*100))
        game_soup = BeautifulSoup(game_req.text, 'html.parser')
        print(game_url)
        user_score = game_soup.select('div.feature_userscore')[0].select(
            '.metascore_w.user.large.game')[0].text
        publisher = game_soup.select('li.publisher')[0].find("a").text.strip()
        platforms_count = len(game_soup.select("li.product_platforms")[0].find_all(
            "a"))+1 if game_soup.select("li.product_platforms") else 1
        developer = game_soup.select('li.developer')[0].find("a").text
        awards_count = len(game_soup.find_all("div", class_="ranking_title"))
        mature_rating = game_soup.select("li.product_rating")[0].select(
            "span.data")[0].text if game_soup.select("li.product_rating") else ''

        # Reviews
        # game_url = base_url+''+href+'/user-reviews'
        # game_req = requests.get(game_url, headers=headers)
        # game_soup = BeautifulSoup(game_req.text, 'html.parser')
        critic_reviews_count = game_soup.select("div.metascore_summary")[0].find(
            "span", class_="count").find("a").find("span").text.strip()
        # user_reviews_count = re.search(r"\d+", game_soup.select("div.side_details")[0].find("span", class_="count").find("a").text).group(0)

        # add the values per game
        record.update({'userscore': user_score, 'awards_count': awards_count, 'publisher': publisher, 'developer': developer,
                       'critic_reviews_count': critic_reviews_count, 'platforms_count': platforms_count, 'mature_rating': mature_rating})
        game_records.append(record)
        index = index + 1

    new_data = pd.DataFrame(game_records)

    if not new_data.empty:
        current_data = current_data.append(new_data, ignore_index=True)
        current_data.to_csv("./video_games_ratings.csv",  mode='w')
    return current_data


df = metacritic_games_data(2, 3)
df
