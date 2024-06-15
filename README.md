# SparkDocRanker: Document Ranking with Apache Spark

## Overview
SparkDocRanker is a big data project designed to implement a batch-based text search and filtering pipeline using Apache Spark. The core objective is to rank a large set of text documents by relevance to user-defined queries, while removing overly similar documents in the final ranking. This project showcases efficient big data processing techniques, adhering to software engineering best practices, and aims to deliver high performance in handling large datasets.

## Features
- **Batch-based Text Search**: Processes and ranks documents based on user queries.
- **Stopword Removal and Stemming**: Preprocesses text to enhance search accuracy.
- **DPH Ranking Model**: Utilizes the Divergence from Randomness (DFR) model to score document relevance.
- **Redundancy Filtering**: Removes near-duplicate documents from the final results.
- **Apache Spark**: Leverages Spark for distributed data processing and scalability.

## Dataset
The dataset comprises news articles from the Washington Post. Two versions are available:
- **Sample Dataset**: Contains 5,000 articles for quick iteration and local testing.
- **Full Dataset**: Contains approximately 670,000 articles for comprehensive evaluation.

## Project Structure
- **data/**: Sample data files for local testing.
- **src/**: Source code for Spark transformations and actions.
- **docs/**: Documentation and report files.
- **scripts/**: Utility scripts for data preprocessing and deployment.

## Getting Started

### Prerequisites
- Java 8 or higher
- Apache Spark 3.0 or higher
- Maven
- Git
