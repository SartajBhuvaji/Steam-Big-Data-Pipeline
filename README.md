# Steam-Data-Engineering-Project

## Project Overview
Welcome to the heart of real-time data engineering—our project dedicated to unraveling the gaming wonders of Steam.com. As one of the paramount digital distribution platforms for PC gaming, Steam sets the stage for our data orchestration. Brace yourself for a journey powered by Kafka, Spark, Airflow, and AWS.

## Kafka Spotlight 🌟
Hey there, data enthusiast! Let's shine a light on Kafka, the backbone of our data collection.

## The Pipeline Trio 🚀
Three pipelines, three different cadences—daily, weekly, and monthly. This setup ensures a sepration of concerns and steady flow of fresh data. 

### Daily Rhythms 🌅
- Source:
    - Most Played Games : https://store.steampowered.com/charts/mostplayed
- Purpose: Collects daily data on the most played games.

Every day, the curtain rises on the gaming stage. Behold the Most Played Games list at Steam.com. With the finesse of web scraping and the power of Kafka, we've crafted a daily symphony. The Daily Kafka Producer takes the spotlight, bringing the latest player counts, game ranks, and more to the forefront. 

### Weekly Data Wonders 🌈
- Sources:
    - Top Sellers in the US
    - Steam News
    - App Reviews
- Purpose: Gathers weekly insights by aggregating data from top sellers, game news, and user reviews.    

Our Weekly Data Wonders unfold from the WEEKLY TOP SELLERS, showcasing the top 100 games in revenue. Armed with App IDs, we delve into game news for updates and bug fixes straight from the developers. Simultaneously, we tap into the community's heartbeat—user reviews from the app page, offering a valuable pulse on user sentiments.

### Monthly Data Marvels 🚀
- Source: SimilarWeb API
- Purpose: Collects monthly data related to network traffic, page visits, and engagement metrics, enriching our understanding of Steam's audience.

Powered by Steam's API, our Monthly Data Marvels unveil a backstage pass to Steam's audience spectacle. Network traffic source, page visits, page hops, and other engagement metrics paint a vibrant canvas, helping us decipher the diverse audience that flocks to Steam.

* <i>Note: Although I wanted to collect more data to process, the other options are paid, but they provide great insights. If you want to have an intense collection of data, you can refer to this link :www.demourl.com</i>


## PySpark and Airflow Data Symphony 🚀🔧
Pyspark was used to process the data and Airflow was used to develop DAGs

### Local and Cloud Vibes ☁️🖥️
Our project is versatile, ready to run on both- local machines and in the expansive AWS cloud. Let's dive into the execution intricacies.

When running locally the data from Apache Consumer is stored inside the data folder in the following structure. (Attach github link here)
If running on AWS, the data is stored in S3 bucket named 'steam-raw-storage' 

Once raw data is stored, I also have a shell script that runs to create a back-up of the raw data. Note: THis would be trigerred by Airflow later. The backup script creates a copy of this raw data and stores it locally or on S3 bucket named 'steam-raw-store-backup'.

Once I have collected raw data, I use Apache Spark to process the data. The code to spark scripts can be found here(Insert github link to spark code). According to the data collected (daily/weekly/monthly) I then run the spark script that parses the data, cleans it and stores it in a easy to use format. When using Airflow, this will be trigreed after raw data is backed up.


## House Keeping ♻️
Once Spark jobs complete, the cleand data is stored in folder 'cleaned_data'(if running locally) or the AWS S3 Bucket 'steam-clean-storage'. Using a different shell script, this cleaned data is also backed up at 'cleaned_data_backup' or S3 bucket 'steam-clean-storage-backup'. (Back ups are important 😄) 

Once data is backed up, I run the pipeline housekeeping Job, which deletes data from primary folders, but maintains data on the back up folders.


## Visualizations 🖥️
Now, I use Tableau(if running locally) or AWS QuickSight to visualize the data whcih gets trigerred when new data is found in S3


## Local Setup
### Setup Kafka

### Setup Airflow

- Install Pyspark