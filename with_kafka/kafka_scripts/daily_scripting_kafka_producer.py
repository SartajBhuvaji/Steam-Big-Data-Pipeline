from selenium.webdriver.common.by import By
from WebDriverCreation import WebDriverCreation
import pandas as pd
import time
from kafka import KafkaProducer

class MostPlayedGamesProducer:
    def __init__(self) -> None:
        self.driver_instance = WebDriverCreation()
        self.wd = self.driver_instance.wd
        self.url = "https://store.steampowered.com/charts/mostplayed"
        self.games_list = []
        self.games = []
        self.collection_date = pd.to_datetime('today').strftime("%Y-%m-%d")
        self.kafka_bootstrap_servers = 'localhost:9092'
        self.kafka_topic = 'most_played_games'

    def scroll_page(self, wd):
        '''Takes the driver as an input and simulates the scrolling to get all the games'''
        SCROLL_PAUSE_TIME = 2
        last_height = wd.execute_script("return document.body.scrollHeight")

        for _ in range(6):
            wd.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_PAUSE_TIME)
            new_height = wd.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    def get_data(self):
        self.wd.get(self.url)
        self.scroll_page(self.wd)
        print(self.wd.title)

    def get_games(self):
        game_rows = self.wd.find_elements(By.CLASS_NAME, 'weeklytopsellers_TableRow_2-RN6')
        for game in game_rows:
            self.games_list.append(str(game.text).split('\n'))
        for game in self.games_list:
            flag = 0
            if 'Free To Play' in game:
                flag = 1
            temp_count = game[-1]
            current_players, peek_today = temp_count.split(" ")
            self.games.append([game[0], game[1], flag, current_players, peek_today])
        return self.games

    def produce_to_kafka(self):
        producer = KafkaProducer(bootstrap_servers=self.kafka_bootstrap_servers)
        for game in self.games:
            message = ','.join(str(field) for field in game).encode('utf-8')
            producer.send(self.kafka_topic, value=message)
        producer.flush()

if __name__ == "__main__":
    obj = MostPlayedGamesProducer()
    obj.get_data()
    games = obj.get_games()
    obj.produce_to_kafka()
