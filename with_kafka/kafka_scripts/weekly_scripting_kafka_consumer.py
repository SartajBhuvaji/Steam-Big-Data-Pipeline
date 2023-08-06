from kafka import KafkaConsumer
import pandas as pd
# Create a Kafka consumer
from datetime import date
collection_data = date.today()

consumer = KafkaConsumer(
    'weekly_top_sellers_games',
    'weekly_top_sellers_app_id',
    'weekly_reviews',
    'weekly_news',
    bootstrap_servers='localhost:9092',
    group_id='weekly_data_group'
)

# Consume messages from Kafka topics
for message in consumer:
    topic = message.topic
    data = message.value.decode('utf-8')
    
    if topic == 'weekly_top_sellers_games':
        game_list = []
        print(f"Received top sellers data")
        for rows in eval(data):
            game_list.append(rows)

        df = pd.DataFrame(game_list, columns=['Rank', 'Game Name', 'Free to Play'])
        df.to_csv(f'../data/weekly_data/top_sellers/{collection_data}_weekly_top_sellers.csv', index=False)
    
    elif topic == 'weekly_top_sellers_app_id':
        print(f"Received app ids:")
        appid_list = []
        for rows in eval(data):
            appid_list.append(rows)
        df = pd.DataFrame(appid_list, columns=['App ID'])
        df.to_csv(f'../data/weekly_data/top_sellers/{collection_data}_weekly_top_sellers_appIds.csv', index=False)
    
    elif topic == 'weekly_news':
        # Process news data
        print(f"Received news data: {data}")
        # Add your processing logic here
