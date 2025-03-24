import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WindDirectionReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        ArrayList<Integer> windDirections = new ArrayList<>();

        // Collect all wind direction values for the key
        for (IntWritable value : values) {
            windDirections.add(value.get());
        }

        // Sort the list to calculate the median
        Collections.sort(windDirections);
        double median;
        int size = windDirections.size();

        if (size % 2 == 0) {
            median = (windDirections.get(size / 2 - 1) + windDirections.get(size / 2)) / 2.0;
        } else {
            median = windDirections.get(size / 2);
        }

        // Emit the year-month and the median wind direction
        context.write(key, new DoubleWritable(median));
    }
}
