import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.Paths;

public class Hits extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(SortJob.class);

    public static class HitsMapper extends Mapper<Text, Text, Text, Text> {
        protected void map(Text url, Text data, Context context) throws IOException, InterruptedException {
            // In:
            //          h a               out links              in links
            // url1     2 3	    ###	    url2	url4	###	   url2	    url3

            // Out:
            // url1	    ###	    url2	url4	###	   url2	    url3
            // url2	    a2
            // url4	    a2
            // url2	    h3
            // url3	    h3

            String[] data_split = data.toString().split("###");
            String[] h_a = data_split[0].trim().split("\t");
            String[] out = data_split[1].trim().split("\t");
            String[] in = data_split[2].trim().split("\t");
            String h = "1";
            String a = "1";

            // На первой итерации для всех документов ставим авторитетность и хабность равной 1.
            Configuration conf = context.getConfiguration();
            if (!conf.get("isFirst").equals("1")) {
                h = h_a[0];
                a = h_a[1];
            }

            // Исходящим ссылкам (out) из текущего документа (url) надо знать его хабность, что бы посчитать свою авторитетность.
            // Поэтому всем исходящим ссылкам присваиваем значение хабности документа (url), что бы они расчитали свою авторитетность.
            // Входящим ссылкам (in) в текущий документ (url) надо знать его авторитетность, что бы посчитать свою хабность.
            // Поэтому всем входящим ссылкам присваиваем значение аторитетности документа (url), что бы они расчитали свою хабность.

            for (String item : out) {
                if (!item.trim().isEmpty()) {
                    context.write(new Text(item), new Text("a" + h));
                }
            }
            for (String item : in) {
                if (!item.trim().isEmpty()) {
                    context.write(new Text(item), new Text("h" + a));
                }
            }
            context.write(new Text(url), new Text("###\t" + String.join("\t", out) + "\t###\t" + String.join("\t", in)));
        }
    }

    public static class HitsReducer extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text url, Iterable<Text> data, Context context) throws IOException, InterruptedException {
            double h = 0;
            double a = 0;
            String out_in = "";

            for (Text item: data) {
                char who = item.toString().trim().charAt(0);
                String content = item.toString().trim().substring(1);
                if (who == 'h') {
                    h += Double.valueOf(content);
                } else if (who == 'a') {
                    a += Double.valueOf(content);
                } else {
                    out_in = item.toString();
                }
            }

            context.write(url, new Text(h + "\t" + a + "\t" + out_in));
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

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, InputPath);
        FileOutputFormat.setOutputPath(job, OutputPath);

        Configuration configuration = job.getConfiguration();
        configuration.set("isFirst", args[2]);

        // Mapper
        job.setMapperClass(HitsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Reduce
        job.setReducerClass(HitsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String InputPath = args[0];
        String OutputPath = args[1];
        String isFirst = "1";
        int iter = 5;
        int rc = 1;

        for (int i = 1; i <= iter; i += 1) {
            String folderName = "iter" + i;
            if (iter == i) {
                // Спец название, что бы знать где брать финальный результат
                folderName = "iter_last";
            }
            System.out.println(new Text("--Start " + i + " iteration"));
            String OutputPathNew = Paths.get(OutputPath, folderName).toString();
            rc = ToolRunner.run(new Hits(), new String[] {InputPath, OutputPathNew, isFirst});
            InputPath = OutputPathNew;
            isFirst = "0";
        }

        System.exit(rc);
    }
}