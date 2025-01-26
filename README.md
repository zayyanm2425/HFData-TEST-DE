# HelloFresh Data Engineering Test

This project is a solution to the HelloFresh Data Engineering Test. It processes recipe data using Apache Spark and Python, performing ETL operations and generating structured output for analysis.

---

## **Table of Contents**
- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Dependencies](#dependencies)
- [Installing Hadoop and PySpark](#installing-hadoop-and-pyspark)
- [Project Structure](#project-structure)
- [Instructions to Run the Project](#instructions-to-run-the-project)
- [Outputs](#outputs)
- [Testing](#testing)

---

## **Overview**
This project involves:
1. **Data Preprocessing**:
   - Reading recipe data from JSON files.
   - Cleaning and transforming the data.
   - Writing processed data to Parquet format.
2. **Data Analysis**:
   - Filtering recipes containing "beef."
   - Calculating the average cooking time for recipes by difficulty level.
   - Writing results as CSV files.

---

## **Prerequisites**
Before running the project, ensure you have:
- Python 3.9 or above.
- Java 8 or 11 (required for Hadoop and Spark).
- Git installed.
- A Bash shell or terminal.

---

## **Dependencies**
Install the required Python libraries:
```bash
pip install pyspark pandas pytest
