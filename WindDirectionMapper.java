import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WindDirectionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int MISSING = 999;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        
        // Extract year and month (positions 15-20)
        String yearMonth = line.substring(15, 21);
        
        // Extract wind direction (positions 60-63) and quality code (position 64)
        int windDirection = Integer.parseInt(line.substring(60, 63));
        String quality = line.substring(63, 64);

        // Ignore invalid wind direction (999) or bad quality codes
        if (windDirection != MISSING && quality.matches("[01459]")) {
            context.write(new Text(yearMonth), new IntWritable(windDirection));
        }
    }
}
