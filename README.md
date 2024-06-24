# Provider Visits

This is a Spark project used to generate reports that deliver data about providers.

## Requirements

- **Language**: Scala
- **Framework**: Spark
- **Output**: JSON file/files

## Data

Ensure the following data files are present in the `data` subfolder:

1. `providers.csv` - A CSV containing data about providers and their respective specialties.
2. `visits.csv` - A CSV with the unique visit ID, the provider ID (the ID of the provider who was visited), and the date of service of the visit.

## Problems

1. **Calculate the total number of visits per provider.** The resulting set should contain the provider's ID, name, specialty, along with the number of visits. Output the report in JSON, partitioned by the provider's specialty.
2. **Calculate the total number of visits per provider per month.** The resulting set should contain the provider's ID, the month, and total number of visits. Output the result set in JSON.

## How to Run the Project

1. **Ensure the Required Data Files are Present:**
   Ensure that `providers.csv` and `visits.csv` are located in the `data` subfolder.

2. **Build and Run the Project:**
   ```bash
   sbt clean compile
   sbt run
   ```

## Approach to the Solution

1. **Read Data:**
    - Read `providers.csv` and `visits.csv` into DataFrames using Spark.

2. **Problem 1: Total Visits Per Provider**
    - Join the `visits` DataFrame with the `providers` DataFrame on `provider_id`.
    - Group by `provider_id` and aggregate the count of visits.
    - Select relevant columns and write the result to JSON, partitioned by `provider_specialty`.

3. **Problem 2: Total Visits Per Provider Per Month**
    - Convert `date_of_service` to month format.
    - Group by `provider_id` and month, and aggregate the count of visits.
    - Write the result to JSON.