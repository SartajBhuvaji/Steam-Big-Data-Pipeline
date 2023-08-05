from kafka import KafkaConsumer
import pandas as pd

class MostPlayedGamesConsumer:
    def __init__(self) -> None:
        self.kafka_bootstrap_servers = 'localhost:9092'
        self.kafka_topic = 'most_played_games'
        self.collection_date = pd.to_datetime('today').strftime("%Y-%m-%d")

    def consume_from_kafka(self):
        consumer = KafkaConsumer(self.kafka_topic, bootstrap_servers=self.kafka_bootstrap_servers)
        games = []
        for message in consumer:
            game_data = message.value.decode('utf-8').split(',')
            games.append(game_data)
        consumer.close()
        return games

    def save_as_csv(self, games):
        df = pd.DataFrame(games, columns=['Rank', 'Game Name', 'Free to Play', 'Current Players', 'Peek Today'])
        df['Collection Date'] = self.collection_date
        df.to_csv(f'../data/daily_data/most_played/{self.collection_date}_MostPlayed_Consumed.csv', index=False)

if __name__ == "__main__":
    obj = MostPlayedGamesConsumer()
    games = obj.consume_from_kafka()
    obj.save_as_csv(games)
