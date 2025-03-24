import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WindDirection {

    public static void main(String[] args) throws Exception {
        // Ensure input and output paths are provided
        if (args.length != 2) {
            System.err.println("Usage: WindDirection <input path> <output path>");
            System.exit(-1);
        }

        // Configure the job
        Job job = new Job();
        job.setJarByClass(WindDirection.class);
        job.setJobName("Wind Direction");

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set Mapper and Reducer classes
        job.setMapperClass(WindDirectionMapper.class); // Your Mapper class
        job.setReducerClass(WindDirectionReducer.class); // Your Reducer class

        // Set output key and value types
        job.setOutputKeyClass(Text.class); // Year-Month as Text
        job.setOutputValueClass(IntWritable.class); // Wind directions as IntWritable

        // Submit the job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
