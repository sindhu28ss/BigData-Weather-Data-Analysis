from pyspark import SparkContext
import sys

def main():
    # Initialize SparkContext
    sc = SparkContext(appName="VisibilityRangeByStation")

    # Input file path from command-line arguments
    input_path = sys.argv[1]
    input_rdd = sc.textFile(input_path)

    # Filter out records with missing visibility or invalid quality codes
    filtered_rdd = input_rdd.filter(lambda line: line[78:84] != "999999" and line[84:85] in "01459")

    # Extract (USAF_ID, visibility) as key-value pairs
    station_visibility_rdd = filtered_rdd.map(lambda line: (line[4:10], int(line[78:84])))

    # Group visibility distances by station ID and calculate the range (max - min)
    visibility_range_rdd = station_visibility_rdd.groupByKey().mapValues(lambda vis: max(vis) - min(vis))

    # Output file path from command-line arguments
    output_path = sys.argv[2]
    visibility_range_rdd.map(lambda x: "{}\t{}".format(x[0], x[1])).saveAsTextFile(output_path)

    # Stop the SparkContext
    sc.stop()

if __name__ == "__main__":
    main()
