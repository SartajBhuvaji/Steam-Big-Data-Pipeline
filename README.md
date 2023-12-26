# Steam-Data-Engineering-Project

## Project Overview
Welcome to the heart of real-time data engineering—our project dedicated to unraveling the gaming wonders of [Steam](https://store.steampowered.com/). As one of the paramount digital distribution platforms for PC gaming, Steam sets the stage for our data orchestration. Brace yourself for a journey powered by Kafka, Spark, Airflow, and AWS, where we would perform data Extraction, Transformation, and Loading (ETL).

## Diagram

## Kafka Spotlight 🌟
Hey there, data enthusiast! Let's shine a light on Kafka, the backbone of our data collection. To use Kafka, I have set up a simple producer-consumer schema for each web page. The producer scrapes the web page or collects data through Steam's APIs. This data is consumed by a consumer who then stores the data accordingly.

## The Pipeline Trio 🚀
Three pipelines, three different cadences—daily, weekly, and monthly. This setup ensures a separation of concerns and a steady flow of fresh data. 

### Daily Rhythms 🌅
- Source:
    - [Most Played Games](https://store.steampowered.com/charts/mostplayed)
- Purpose: Collect daily data on the most played games.

Every day, the curtain rises on the gaming stage. Behold the Most Played Games list from Steam. With the finesse of web scraping and the power of Kafka, the Daily Kafka Producer takes the spotlight, bringing the latest player counts, game ranks, and more to the forefront. 

### Weekly Data Wonders 🌈
- Sources:
    - [Top Sellers in the US](https://store.steampowered.com/charts/topsellers/US)
    - [Steam News](http://api.steampowered.com/ISteamNews/GetNewsForApp/v0002/?appid=)
    - [App Reviews](https://store.steampowered.com/appreviews/)
- Purpose: Gathers weekly insights by aggregating data from top sellers, game news, and user reviews.    

Our Weekly Data Wonders unfold from the WEEKLY TOP SELLERS, showcasing the top 100 games in revenue. Armed with App IDs, we delve into game news for updates and bug fixes straight from the developers. Simultaneously, we tap into the community's heartbeat—user reviews from the app page, offering a valuable pulse on user sentiments.

### Monthly Data Marvels 🚀
- Source: 
    - [Monthly Visits](https://data.similarweb.com/api/v1/data?domain=store.steampowered.com)
- Purpose: Collects monthly data related to network traffic, page visits, and engagement metrics, enriching our understanding of Steam's audience.

Powered by Steam's API, our Monthly Data Marvels unveil a backstage pass to Steam's audience spectacle. Network traffic sources, page visits, page hops, and other engagement metrics paint a vibrant canvas, helping us decipher the diverse audience that flocks to Steam.

* <i> Note: Although I wanted to collect more data to process, the other options are paid, but they provide great insights. If you want to have an intense collection of data, you can refer to this [link](https://data.similarweb.com/api/v1/data?domain=store.steampowered.com) </i>

## PySpark and Airflow Data Symphony 🦄
I use  PySpark to process our data seamlessly. The magic doesn't stop there— Airflow joins the orchestra, orchestrating the entire data flow with its slick Directed Acyclic Graphs (DAGs). It's a symphony of efficiency and elegance, making data management a breeze.

### Local and Cloud Vibes ☁️🖥️
Our project is versatile and ready to run on both- local machines and in the expansive AWS cloud. Let's dive into the execution intricacies.

When running locally the data from Kafka Consumer is stored inside a data folder in the following structure. (Attach GitHub link here)
If running on AWS, the data is stored in an S3 bucket named 'steam-raw-storage' 

Once raw data is stored, I also have a shell script that runs to create a backup of the raw data. Note: This would be triggered by Airflow later. The backup script creates a copy of this raw data and stores it locally or on an S3 bucket named 'steam-raw-store-backup'.

Once I have back up the raw data, I use Apache Spark to process it. The code to spark scripts can be found here.(Insert GitHub link to spark code) According to the data collected (daily/weekly/monthly), I then run the spark script that parses the data, cleans it, and stores it in an easy-to-use format. When using Airflow, this will be triggered after raw data is backed up.

### Airflow DAG 🧑🏻‍🔧
Airflow DAGs are the choreographers of our data dance! 🕺💃

These Directed Acyclic Graphs (DAGs)🕸️ are like the master conductors, guiding the flow of our data tasks. They define the order and dependencies, ensuring each step pirouettes gracefully into the next. Whether it's orchestrating data backups, triggering PySpark scripts, or managing the entire data symphony, Airflow DAGs make it happen.

### Back Up 🦺
After the PySpark magic wraps up, the cleaned data finds its cozy spot in the 'cleaned_data' folder (for local runs) or gets whisked away to the AWS S3 Bucket 'steam-clean-storage'. But hey, we're all about that backup life! 🧼✨

Cue the backup script—it ensures the cleaned data has a secure twin at 'cleaned_data_backup' or the 'steam-clean-storage-backup' S3 bucket. Because, let's face it, backups are the unsung heroes of data life! 🚀(Backups are important 😄) 

### House Keeping♻️
With data safely tucked into its backup haven, it's time for a bit of digital tidying! 🧹 

The pipeline housekeeping job steps in, gracefully clearing out the primary folders. But fear not, our data superheroes stay intact in the backup folders, ready to save the day when needed! 🦸‍♂️📦

### What is something breaks? 🫗
In my previous role, I managed a few pipelines and I know stuff breaks. Picture this: It's Friday evening and your PySpark script suddenly decides it wants a day off, maybe due to unexpected changes in the data source or a cosmic hiccup. It's a data world, after all! 🌌

But fear not! Introducing the superheroes of recovery—the Catch-Up DAGs! 🦸‍♀️💨

These mighty DAGs are your safety nets, ready to swoop in and save the day. Triggered manually, they're designed to catch up on any missed tasks, ensuring your data pipeline gets back on track. Each pipeline has its separate DAG and each DAG has separate stages that can help you run each part of the pipeline independently. Catch Up DAG Because even in the face of surprises, I've got your back! 💪✨

## Visualizations 👀
Time to bring the data to life! 🚀📊

Whether it's the local vibes with Tableau or the cloud magic with AWS QuickSight, our visualizations are the grand finale. And guess what? AWS is all automated! When the data universe detects something new in the S3 storage, these visualization wizards kick into action, turning raw data into insights you can feast your eyes on. 🎉

## Local Setup
- Setup Kafka
- Install PySpark `pip install pyspark`
- Setup Airflow

### Local Setup Issues
Setting up locally is an easy way, however you might face some issues. I use a Windows machine and I have used this video to set up Kafka. However, Airflow does not work natively on Windows 🥲 The easiest workaround is using Docker 🐋. You can refer to the docker image here. But now you need to have Kafka and PySpark set up in your Docker too. You'd need to find an image that has: Kafka + PySpark + Airflow. This makes the docker container too heavy(16GB+ RAM) and would not run on my laptop. So you can implement the project in parts. Having Kafka run locally. This would help you get raw data. On your raw data, you can build a docker image with Airflow and PySpark, transfer the raw data, and run the DAGs to achieve the cleaned data. Then you'd need to transfer the clean data back to your drive and use Tableau to visualize the results.😤 OOF. 

### Setting Up Docker 🐳
Check the awesome DOCKER-README.me file  

### AWS to the rescue! 🤌🏻
{Write AWS Setup here}


## Screenshots

### AirFlow DAGS
![af0](https://github.com/SartajBhuvaji/Steam-Big-Data-Pipeline/assets/31826483/d57fdf20-204c-4fe9-8665-27f7b2d2a79c)

![af1](https://github.com/SartajBhuvaji/Steam-Big-Data-Pipeline/assets/31826483/898adb5b-2967-4c93-ad70-ea6b7bb332ee)

![af2](https://github.com/SartajBhuvaji/Steam-Big-Data-Pipeline/assets/31826483/03a99104-63cc-4c08-a1c7-1babdd8b45ab)

![af3](https://github.com/SartajBhuvaji/Steam-Big-Data-Pipeline/assets/31826483/db3a81f5-603a-4e12-af98-36e78f7b6285)




### AWS S3 Buckets

### QuickSight Visualizations
