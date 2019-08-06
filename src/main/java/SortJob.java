import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import java.io.IOException;

public class SortJob extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(SortJob.class);

    // Mapper
    static public class MapperSort extends Mapper<Text, Text, DoubleWritable, Text> {
        protected void map(Text offset, Text data, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String[] data_split = data.toString().split("###");
            String[] scoreName = data_split[0].trim().split("\t");
            String scoreNameParam = conf.get("scoreNameParam");
            double score = 0;

            // h - habs
            // a - authority
            // p - pagerank
            if (scoreNameParam.equals("h") || scoreNameParam.equals("p")) {
                score = -Double.parseDouble(scoreName[0]);
            } else if (scoreNameParam.equals("a")) {
                score = -Double.parseDouble(scoreName[1]);
            }

            context.write(new DoubleWritable(score), new Text(offset));
        }
    }

    // Reducer
    public static class ReducerSort extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
        protected void reduce(DoubleWritable score, Iterable<Text> links, Context context) throws IOException, InterruptedException {
            for (Text link : links) {
                context.write(score, link);
            }
        }
    }

    public int run(String[] args) throws Exception {
        Path InputPath = new Path(args[0]);
        Path OutputPath = new Path(args[1]);

        FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(OutputPath)) {
            fs.delete(OutputPath, true);
        }

        Job job = Job.getInstance(getConf(), "");
        job.setJarByClass(this.getClass());

        Configuration configuration = job.getConfiguration();
        configuration.set("scoreNameParam", args[2]);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, InputPath);
        FileOutputFormat.setOutputPath(job, OutputPath);

        // Mapper
        job.setMapperClass(MapperSort.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        // Reduce
        job.setReducerClass(ReducerSort.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new SortJob(), args);
        System.exit(rc);
    }
}