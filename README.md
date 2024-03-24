## Introduction
The project encompasses a comprehensive suite of modules designed to process, merge, and manage data streams effectively, leveraging the power of Apache Spark and orchestrated by Apache Airflow. Each module plays a specific role in the data pipeline, from ingestion and processing to merging and analytics, ensuring that data is accurate, consistent, and timely. Here’s an introduction to each module within the context of the entire project:

## Versions
- Scala 2.11.12
- Spark 2.4.4
- Kafka 0.10
- Airflow 1.10.12

## Overview
This sophisticated data processing ecosystem is engineered to handle vast volumes of data across various streams, such as blogs, posts, and metrics. Built on Apache Spark for its processing capabilities and Apache Airflow for workflow orchestration, the project addresses complex data transformation, merging, and scheduling needs. The goal is to provide a robust framework for data analytics, enabling insights into content performance, user engagement, and system metrics.

## Modules
Modules (`blogsWorkflowStream`, `metricsWorkflowStream`, `postsWorkflowStream`, `streamsWorkflowMerger`)

These modules are Spark-based applications tailored for heavy-duty data processing tasks. They include functionality for data ingestion, transformation, and preliminary analysis. Designed to run efficiently on distributed systems, these modules can process data from diverse sources, preparing it for further analysis and integration.

- `BlogsWorkflowStream`: processes streaming data from blogs, enriching and structuring it for analytics.
- `PostsWorkflowStream`: handles data related to posts, performing necessary transformations for consistency and clarity.
- `MetricsWorkflowStream`: focuses on metrics data, ensuring it's accurately processed to reflect user engagement and system performance.
Airflow Python Files

## Airflow DAGS
The Airflow DAGs (Directed Acyclic Graphs) define and schedule workflows for each Spark job, managing dependencies and execution order. They ensure that data flows seamlessly through the pipeline, from ingestion to final storage, with robust error handling and retry mechanisms.

- Blogs data processing workflow: orchestrate the blogs data processing, ensuring data is ingested, processed, and stored correctly.
- Posts data management workflow: manages the workflow for posts data, coordinating between ingestion, processing, and storage tasks.
- Metrics data analysis workflow: automates the processing of metrics data, facilitating analytical tasks and storage.
`streamsWorkflowMerger`

A key component of the project, this module merges processed data from blogs, posts, and metrics, ensuring a unified view of the data. It handles complex join operations, data deduplication, and historical data integration, making it a cornerstone for analytics and reporting.

- `Merge strategy`: employs sophisticated logic to combine data streams, ensuring accuracy and completeness.
- `Airflow orchestration`: includes a detailed DAG to manage the merge process, handling dependencies, scheduling, and error management.

## Project significance
This project stands out for its ability to process and manage large-scale data pipelines, offering a scalable solution for data analytics and insight generation. By leveraging Spark for data processing and Airflow for workflow management, it addresses the challenges of big data environments, including efficiency, reliability, and scalability.

Designed with modularity and extensibility in mind, each component of the project contributes to a cohesive data processing pipeline. Users can expect a system that not only meets the demands of current data processing tasks but also adapts to future requirements, facilitating ongoing data analysis and decision-making.

## blogsWorkflowStream module

### Introduction

The `blogsWorkflowStream` module is designed for processing streaming blog data. Utilizing Apache Spark, it reads from Kafka topics, processes the streaming data according to predefined logic, and then writes the results to HDFS, managing Kafka offsets to ensure data integrity and fault tolerance. This module is an integral part of a larger data processing pipeline that includes handling complex blog data transformations.

### Configuration

Before running the `blogsWorkflowStream` module, ensure that the Kafka and HDFS configurations are correctly set up in the BlogsWorkflowStreamConfig class. This includes Kafka bootstrap servers, consumer group ID, topic name, and paths for data and offset storage in HDFS.

### Airflow Integration

The `blogsWorkflowStream` module is designed to be orchestrated with Apache Airflow, allowing for automated scheduling, execution, and monitoring of the blog data processing workflow. The provided Airflow DAG (`blogs_workflow_stream_job_dag` in the /deploy/dags/[blogs_workflow_stream_dag.py](deploy%2Fdags%2Fblogs_workflow_stream_dag.py)) facilitates this integration, leveraging the SparkSubmitOperator for submitting the Spark job.

#### Airflow: parameters and variables

The DAG utilizes Airflow variables (`blogs_workflow_stream_params`) to dynamically pass configuration parameters to the Spark job, including:

- `kafkaHost`: Kafka bootstrap servers address.
- `kafkaConsumerGroup`: Kafka consumer group ID.
- `blogsTopicName`: Name of the Kafka topic for blog data.
- `hdfsPath`: HDFS path for storing processed data.
- `hdfsOffsetsPath`: HDFS path for storing Kafka offsets.

These variables ensure that the DAG is flexible and can be easily adjusted without modifying the DAG code directly.


### Incoming data specification

The module processes data incoming from a Kafka topic dedicated to blog entries. The data is expected to be in JSON format with the following key fields:

- `UrlHash`: Unique identifier of the blog post
- `Url`: URL of the blog post
- `AvatarUrl`: URL of the avatar associated with the blog post
- `CreateDate`: Timestamp of when the blog post was created
- `MetricsAvg1w`: Aggregated metrics of the blog post over the past week, including comments, reposts, views, and likes
- `Metrics`: Current metrics of the blog post, similar to MetricsAvg1w but for the current moment
- `Nick`: Nickname of the blog post author
- `DateOfBirth`: Date of birth of the author
- `EducationLevel`: Education level of the author
- `ProviderType`: The social network provider type of the blog post
- `Type`: The type of the blog post

Each JSON message from Kafka is parsed and transformed to fit a schema that is more suitable for downstream processing and analytics tasks.

### Usage

To run the `blogsWorkflowStream` module, you need to initialize and run the `BlogsWorkflowStreamProcessor` with a properly configured `BlogsWorkflowStreamConfig` instance. Detailed commands and script examples should be provided for starting the job from the command line or a scheduling tool.

### Kafka offsets management integration

The module integrates with a custom Kafka offsets management system to ensure that blog data is processed exactly once and in order, preventing data loss or duplication. The `SparkKafkaCustomOffsetsWriter` and `SparkManualCustomOffsetsManager` classes are used to manage offsets. For details on how these components work, refer to the project's main documentation.


### Task flow

The DAG is structured with a start (`start_blogs_workflow_stream_job`), the Spark job (`blogs_workflow_stream_job`), and an end (`end_blogs_workflow_stream_job`) task, ensuring a clear beginning and conclusion to each execution flow.

## metrics_workflow_stream module

### Introduction

The `metrics_workflow_stream` module is a specialized component of a large-scale data processing system designed to ingest, transform, and store metrics data related to online content. Leveraging Apache Spark's capabilities, this module reads streaming data from a specified Kafka topic, applies necessary transformations to adhere to a predefined schema, and persists the output in HDFS. This process ensures the data is readily available for further analysis or consumption by downstream systems.

### Configuration

The module is configured through the `MetricsWorkflowStreamConfig` class, which specifies parameters such as Kafka connection details, topic names, and HDFS paths. Ensure these configurations align with your environment and Kafka setup.

### Airflow Integration

The `metrics_workflow_stream` module is designed to be fully integrated within an Apache Airflow managed environment. This integration allows for automated, scheduled execution of the metrics data processing workflow, leveraging Airflow's robust scheduling and monitoring capabilities. The provided Airflow DAG (`metrics_workflow_stream_job_dag` in the /deploy/dags/[metrics_workflow_stream_dag.py](deploy%2Fdags%2Fmetrics_workflow_stream_dag.py)) facilitates this integration, leveraging the SparkSubmitOperator for submitting the Spark job.


#### Airflow: parameters and variables

The DAG utilizes Airflow variables (`metrics_workflow_stream_params`) to dynamically pass configuration parameters to the Spark job, including:

- `kafkaHost`: Kafka bootstrap servers address.
- `kafkaConsumerGroup`: Kafka consumer group ID.
- `metricsTopicName`: Name of the Kafka topic for metrics data.
- `hdfsPath`: HDFS path for storing processed data.
- `hdfsOffsetsPath`: HDFS path for storing Kafka offsets.

This setup ensures that key configurations can be modified without direct code changes to the DAG, promoting best practices in configuration management.

### Incoming data specification

The module processes data incoming from a Kafka topic dedicated to metrics entries. The data is expected to be in JSON format.

The incoming data is expected to be in JSON format, with each message encapsulating a set of metrics and associated metadata. Below is a breakdown of the key fields typically included in the incoming data messages:

- `Blog.Type`: The type of content (e.g., blog post, article, video).
- `Blog.Url`: The URL of the content item.
- `CreateDate`: The timestamp indicating when the content was created or published.
- `Metrics`: A nested object containing various metrics:
  - `CommentCount`: The total number of comments on the content.
  - `LikeCount`: The total number of likes.
  - `RepostCount`: The total number of reposts or shares.
  - `ViewCount`: The total number of views.
  - `CommentNegativeCount`: The number of negative comments.
  - `CommentPositiveCount`: The number of positive comments.
  - `Timestamp`: The timestamp for when the metrics were recorded.
- `Type`: Indicates the type of metrics data (e.g., daily, weekly metrics).
- `Url`: The specific URL of the metrics being recorded, which could differ from Blog.Url if tracking subcomponents or interactions of the content.

### Task flow

The DAG defines a simple workflow with a start (`start_metrics_workflow_stream_job`), the execution of the Spark job (`metrics_workflow_stream_job`), and an end task (`end_metrics_workflow_stream_job`), ensuring a straightforward execution path and clear demarcation points for job monitoring and troubleshooting.

## posts_workflow_stream module

### Introduction

The `posts_workflow_stream` module is a sophisticated data processing pipeline designed for handling streaming post data. Utilizing Apache Spark's powerful data processing capabilities, this module reads post data from a Kafka topic, processes and transforms this data according to specified schemas, and writes the output into HDFS for durable storage and further analysis.

### Configuration

The module is configured through the `PostsWorkflowStreamConfig` class, which specifies parameters such as Kafka connection details, topic names, and HDFS paths. Ensure these configurations align with your environment and Kafka setup.

### Airflow integration

The `pots_workflow_stream` module is designed to be fully integrated within an Apache Airflow managed environment. This integration allows for automated, scheduled execution of the metrics data processing workflow, leveraging Airflow's robust scheduling and monitoring capabilities. The provided Airflow DAG (`posts_workflow_stream_job_dag` in the /deploy/dags/[posts_workflow_stream_dag.py](deploy%2Fdags%2Fposts_workflow_stream_dag.py)) facilitates this integration, leveraging the SparkSubmitOperator for submitting the Spark job.

#### Airflow: parameters and variables

The DAG utilizes Airflow variables (`pots_workflow_stream_params`) to dynamically pass configuration parameters to the Spark job, including:

- `kafkaHost`: Kafka bootstrap servers address.
- `kafkaConsumerGroup`: Kafka consumer group ID.
- `postsTopicName`: Name of the Kafka topic for posts data.
- `hdfsPath`: HDFS path for storing processed data.
- `hdfsOffsetsPath`: HDFS path for storing Kafka offsets.

This setup ensures that key configurations can be modified without direct code changes to the DAG, promoting best practices in configuration management.

### Incoming data specification

The `posts_workflow_stream` module processes streaming data representing various types of posts. This data is ingested from a Kafka topic, where each message is a JSON object containing detailed information about a single post, including metadata about the author, metrics related to the post's engagement, and other relevant content attributes.

The incoming data is expected to be in JSON format, with each message encapsulating a set of metrics and associated metadata. Below is a breakdown of the key fields typically included in the incoming data messages:

- `Author`: Contains information about the author of the post, including their URL, type, and social media provider.
- `Blog`: Details about the blog or platform the post was published on, including a unique hash of the blog URL and the blog type.
- `Metrics`: Engagement metrics for the post, such as follower count, comment count, like count, repost count, and view count, along with the timestamp when these metrics were recorded.
- `CreateDate`: The date and time when the post was created.
- `IsSpam`: A boolean indicator of whether the post is considered spam.
- `Topics`: An array of topics associated with the post, each represented by an ID.
- `Type`: The type of the post (e.g., article, blog post, tweet).
- `Url`: The URL of the post.

### Task flow

The DAG defines a simple workflow with a start (`start_posts_workflow_stream_job`), the execution of the Spark job (`posts_workflow_stream_job`), and an end task (`end_posts_workflow_stream_job`), ensuring a straightforward execution path and clear demarcation points for job monitoring and troubleshooting.

## streamsWorkflowMerger module

### Introduction

The `streamsWorkflowMerger` module is designed to integrate and synchronize data from various streams, including blogs, posts, and metrics, into a single, unified dataset. Utilizing Apache Spark's powerful data processing capabilities, this module reads data from specified HDFS paths, performs complex joins and merges, and writes the consolidated output back to HDFS. This process is vital for ensuring data consistency across the platform and providing a holistic view of the data for analysis.

### Configuration

Configure the module through the `StreamsWorkflowMergerConfig` class, which includes paths to source data, locations for merged outputs, and execution parameters such as execution date and lower bound for data processing. Ensure these configurations align with your deployment environment and data organization.

### Airflow integration

The Python Airflow file for the streamsWorkflowMerger module orchestrates the complex process of merging different data streams into a unified dataset using Apache Airflow. It's structured as a Directed Acyclic Graph (DAG) that manages task dependencies, scheduling, and execution parameters.
The provided Airflow DAG (`streams_workflow_merger_job_dag` in the /deploy/dags/[streams_workflow_merger_dag.py](deploy%2Fdags%2Fstreams_workflow_merger_dag.py)) facilitates this integration, leveraging the SparkSubmitOperator for submitting the Spark job.

### Airflow: variables and parameterization
The DAG leverages Airflow Variables to dynamically configure paths, execution flags, and other parameters, making the workflow adaptable to different environments or data processing requirements. 

For example:
- paths to source data (hdfs_posts, hdfs_blogs, hdfs_metrics) and target directories for merged outputs (hdfs_merged_posts, hdfs_merged_blogs).
- execution flags such as skip_trash, determining how old data is handled post-merge.

### Dynamic date handling
The DAG dynamically calculates dates for data processing, using Airflow's templating system to pass execution dates ({{ ds }}, {{ next_ds }}) and custom logic within Python callables to compute specific date ranges or bounds for the merge process.

### Features

- `Data merging`: combines data from blogs, posts, and metrics streams based on configurable criteria, ensuring a comprehensive dataset.
- `Delta processing`: capable of processing data increments, allowing for efficient data updates without reprocessing the entire dataset.
- `Historical data`: incorporates previous data states, enabling trend analysis and historical context.
- `Fault tolerance`: implements robust error handling and logging, ensuring the system's resilience against data inconsistencies and operational anomalies.

### Key tasks
- `Data preparation`: sets up initial configurations and determines data ranges for processing.
- `Merging jobs`: separate Spark jobs for blogs and posts, each responsible for reading, merging, and writing data.
- `Self-check`: determines the path of execution based on the selfOnly flag, which controls whether to proceed with further processing or halt.

### Workflow steps

- `Initialization`: prepare the execution context, including date calculations and parameter settings.
- `Data merging`: execute Spark jobs to merge blogs and posts data with corresponding metrics.
- `Finalization`: conclude the workflow, potentially triggering further actions based on the workflow outcome and configuration flags.

### Detailed overview of merge processes
#### Merge Strategy

The module employs a sophisticated merge strategy that combines current execution day data (delta) with historical data to ensure a complete and up-to-date dataset. This process involves several key steps:

- `Window function application`: utilizes Spark SQL window functions to partition data by a unique identifier (such as accountid or post_id) and orders it by currenttimestamp to ensure the most recent records are prioritized in the merge process.
- `Row numbering and filtering`: assigns row numbers to partitioned data and filters to keep only the most recent record for each unique identifier, effectively deduplicating the dataset.
- `Union of delta and historical data`: performs a union operation between the delta data of the current execution and the historical data from previous executions. This step is crucial for incorporating any updates or new entries while maintaining historical records.
- `Final deduplication`: applies another round of deduplication on the unioned dataset to resolve any duplicates that may have arisen from merging delta and historical data, ensuring a clean and accurate dataset for each execution day.

#### Handling Historical Data

- `Historical data reading`: at the start of the merge process, the module attempts to read historical data for the previous execution day. This step is essential for comparing changes and integrating new data.
- `Fallback mechanism:` if historical data is unavailable (e.g., first run scenario), the module defaults to processing only the delta data, establishing a baseline for future merges.

#### Delta data processing

- `Identification of delta data`: delta data is identified based on the execution date, focusing on the changes or additions since the last execution.
- `Renaming overlapping columns`: to manage schema overlaps between posts and metrics data during the merge, overlapping columns are renamed accordingly to prevent conflicts.
- `Join conditions`: uses full outer joins to combine posts and metrics data, ensuring that records from both datasets are retained even if there's no direct match. This approach is vital for creating a comprehensive view of the data.


#### Post-merge data handling

- `Partitioned writes`: after merging, the data is written back to HDFS, partitioned by relevant identifiers and dates to optimize storage and access patterns.
- `Old data management`: implements a configurable mechanism to either delete old data permanently or move it to a designated trash location, depending on the skipTrash parameter.

#### Optimizations and considerations

- `Performance tuning`: the module leverages Spark's dynamic allocation and serialization features to optimize resource usage and processing speed.
- `Error handling and logging`: comprehensive logging and error handling mechanisms are in place to monitor the merge process and address any issues promptly.
- `Configurability:` provides a high degree of configurability through the `StreamsWorkflowMergerConfig` class, allowing adjustments to paths, schemas, and processing parameters to meet various data and operational requirements.

#### Conclusion
The Airflow Python file for the `streamsWorkflowMerger` module encapsulates a sophisticated data processing workflow, leveraging Apache Airflow's capabilities to manage complex dependencies, scheduling, and execution logic in a scalable and maintainable manner. By carefully structuring the DAG and its tasks, the workflow efficiently merges diverse data streams into a cohesive dataset, ready for analysis or further processing.




