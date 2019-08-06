import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.net.MalformedURLException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import java.net.URL;
import java.util.HashMap;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.zip.Inflater;
import java.util.Base64;
import java.io.ByteArrayOutputStream;
import java.util.zip.DataFormatException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.LinkedList;

public class Graph extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(Graph.class);

    // Mapper
    public static class GraphMapper extends Mapper<Text, Text, Text, Text> {
        HashMap<String, String> idUrl = new HashMap<>();

        protected void setup(Context context) throws IOException {
            Configuration configuration = context.getConfiguration();
            Path path = new Path(configuration.get("idUrlPath"));
            FileSystem fs = path.getFileSystem(configuration);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));

            // Собираем хэш { id=url }
            String line;
            while ((line = br.readLine()) != null) {
                String[] args = line.split("\t");
                String id = args[0];
                String url = args[1];
                idUrl.put(id, url);
            }
        }

        protected void map(Text id, Text htmlBase64, Context context) throws IOException, InterruptedException {
            // Переводим Base64 в байты
            byte[] htmlByte = Base64.getDecoder().decode(htmlBase64.toString());
            int compressedDataLength = 0;
            Inflater compresser = new Inflater();
            compresser.setInput(htmlByte, 0, htmlByte.length);
            ByteArrayOutputStream outRes = new ByteArrayOutputStream(htmlByte.length);
            byte[] result = new byte[1024];
            while (!compresser.finished()) {
                try {
                    // Узнаем длину
                    compressedDataLength = compresser.inflate(result);
                    if (compressedDataLength == 0) {
                        break;
                    }
                    outRes.write(result, 0, compressedDataLength);
                } catch (DataFormatException e) {
                    throw new IOException(e);
                }
            }
            outRes.close();
            compresser.end();
            // Получаем из байтов html
            String HtmlNorm = outRes.toString("UTF-8");

            // Находим все ссылки в документе по регулярному выражению
            Pattern regexPattern = Pattern.compile("<a[^>]+href=[\"']?([^\"'\\s>]+)[\"']?[^>]*>");
            Matcher linkMatcher = regexPattern.matcher(HtmlNorm);

            // Берем по id урл документа
            URL mainUrl = new URL(idUrl.get(id.toString()));
            LinkedList<String> links = new LinkedList<>();
            while (linkMatcher.find()) {
                try {
                    // Добавялем домен к ссылки, если его нет
                    URL passiveUrl = new URL(mainUrl, linkMatcher.group(1));
                    // Оставляем ссылки только с ленты
                    if (passiveUrl.getHost().contains("lenta.ru")) {
                        links.add(passiveUrl.toString());
                    }
                } catch (MalformedURLException ex) {
                    continue;
                }
            }

            // Выводим исходящие ссылки
            for (String link: links) {
                context.write(new Text(mainUrl.toString()), new Text(">" + link));
            }

            // Выводим входящие ссылки
            for (String link: links) {
                context.write(new Text(link), new Text("<" + mainUrl.toString()));
            }
        }
    }

    // Reducer
    public static class GraphReducer extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text mainUrl, Iterable<Text> links, Context context) throws IOException, InterruptedException {
            // In:
            // url1 >url2
            // url2 <url1

            // Out:
            //                    OutLinks             InLinks
            // url1	    ###	    url2    url4    ###     url3

            LinkedList<String> linksIn = new LinkedList<>();
            LinkedList<String> linksOut = new LinkedList<>();

            for (Text link : links) {
                char who = link.toString().trim().charAt(0);
                String content = link.toString().trim().substring(1);
                if (who == '>') {
                    linksOut.add(content);
                } else if (who == '<') {
                    linksIn.add(content);
                }
            }

            // Собираем статистику
            context.getCounter("TOPS", "ALL").increment(1);
            if (linksOut.size() == 0) {
                context.getCounter("TOPS", "HANGING").increment(1);
            }

            context.write(mainUrl, new Text("###\t" + String.join("\t", linksOut) + "\t###\t" + String.join("\t", linksIn)));
        }
    }

    public static Job GetJobConf(Configuration conf, String[] args) throws IOException {
        Path InputPath = new Path(args[0]);
        Path OutputPath = new Path(args[1]);

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(OutputPath)) {
            fs.delete(OutputPath, true);
        }

        Job job = Job.getInstance(conf, "");
        job.setJarByClass(Graph.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, InputPath);
        FileOutputFormat.setOutputPath(job, OutputPath);

        Configuration configuration = job.getConfiguration();
        configuration.set("idUrlPath", args[2]);

        // Mapper
        job.setMapperClass(GraphMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Reduce
        job.setReducerClass(GraphReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    public int run(String[] args) throws Exception {
        Job job = GetJobConf(getConf(), args);
        int result = job.waitForCompletion(true) ? 0 : 1;
        Counters counters = job.getCounters();
        long topsAll = counters.findCounter("TOPS", "ALL").getValue();
        long topsHanging = counters.findCounter("TOPS", "HANGING").getValue();
        System.out.println("All count: " + topsAll + "\n" + "Hanging count: " + topsHanging);
        return result;
    }

    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new Graph(), args);
        System.exit(rc);
    }
}