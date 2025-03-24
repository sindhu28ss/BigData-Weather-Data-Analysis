# BigData-Weather-Data-Analysis
This project demonstrates the powerful integration of multiple Big Data technologies to extract, process, and derive meaningful insights from NCDC weather data. Each stage of the project leverages specific tools to manage various aspects of data processing, from extraction and cleaning to aggregation and reporting.
<p align="center">
  <img src="https://github.com/sindhu28ss/BigData-Weather-Data-Analysis/blob/main/output%20files/image.webp" width="300">
</p>

## Data Extraction and Initial Processing

**Tools Used:** Hadoop, Java

**Objective:** Extract and calculate the median of wind directions for each observation month from each year.

**Implementation:**
- Developed Java MapReduce applications: `WindDirection.java`, `WindDirectionMapper.java`, `WindDirectionReducer.java`.
- Compiled and packaged into a JAR.
- Executed on Hadoop to process data stored in HDFS.

## Advanced Analysis with PySpark

**Tools Used:** PySpark

**Objective:** Calculate the range of visibility distances for each USAF weather station ID.

**Implementation:**
- Initiated a PySpark session and executed the `VisDistance.py` script to compute visibility ranges.
- Stored and managed results within HDFS for further analysis.

## Data Enrichment and Aggregation

**Tools Used:** MRJob

**Objective:** Further process visibility data to retrieve detailed metrics per station ID.

**Implementation:**
- Utilized the `mrjob` Python library to run MapReduce jobs directly from the script.
- Facilitated the retrieval and aggregation of data.
- Prepared output data for higher-level analysis.

## Statistical Analysis with Pig

**Tools Used:** Pig

**Objective:** Utilized Pig for ad-hoc queries and calculations, such as deriving the range of visibility distances.

**Implementation:**
- Loaded and processed data in Pig.
- Computed statistical variations across different station IDs.

## Integration and Reporting with Hive

**Tools Used:** Hive

**Objective:** Load processed data into Hive for final aggregation and reporting.

**Implementation:**
- Created Hive tables to store and manage data efficiently.
- Executed SQL-like queries to calculate average visibility distances.

## Conclusion

The project effectively integrates multiple Big Data technologies to derive insights from weather data. Each tool uniquely contributes to handling different aspects of data processing, streamlining the workflow and enhancing the depth and reliability of the analysis.
