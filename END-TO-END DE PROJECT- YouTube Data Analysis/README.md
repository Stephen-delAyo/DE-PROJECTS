**OVERVIEW**
The END-TO-END DATA ENGINEERING PROJECT: YouTube Data Analysis" delves deeply into the comprehensive process of a data engineering endeavor centered on analyzing YouTube data. It spans multiple stages, from data collection and cleaning to analysis, and potentially extends to visualization or the application of machine learning models, all tailored to the extensive dataset of YouTube.
This project strives to securely manage, optimize, and analyze structured and semi-structured data from YouTube videos, focusing on video categories and trending metrics.

**Project Objectives:**
1. Data Ingestion: Develop a mechanism for ingesting data from diverse sources.
2. ETL System: Transform raw data into the appropriate format for analysis.
3. Data Lake: Establish a centralized repository for storing data from multiple sources.
4. Scalability: Ensure the system can scale effectively as data volume grows.
5. Cloud Infrastructure: Utilize cloud services, specifically AWS, to handle large datasets beyond local computing capacity.
6. Reporting: Construct a dashboard to derive insights and answers to previously posed questions.

**Services used include:**
Amazon S3, AWS IAM, AWS Glue, AWS Lambda, and AWS Athena

**Dataset Description:**
The Kaggle dataset utilized in this project consists of statistics in CSV format, documenting daily popular YouTube videos spanning several months. Each day, up to 200 trending videos are featured, covering multiple locations. Data for each region is stored in individual files. The dataset encompasses various attributes such as video title, channel title, publication time, tags, views, likes, dislikes, description, and comment count. Additionally, a category_id field, specific to each region, is provided in the associated JSON file.

