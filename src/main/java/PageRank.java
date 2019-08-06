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

public class PageRank extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(SortJob.class);
    // Количество вершин
    public final static int nNodes = 2925064;
    // Вероятность перехода на любую случайную страницу
    public final static double alpha = 0.1;

    public static double prHangingRead(Configuration conf, String filePath) throws IOException {
        Path path = new Path(filePath);
        FileSystem fs = path.getFileSystem(conf);
        return Double.valueOf(fs.open(path).readUTF());
    }

    public static void prHangingWrite(Configuration conf, String filePath, Double pr) throws IOException {
        Path path = new Path(filePath);
        FileSystem fs = path.getFileSystem(conf);
        fs.create(path).writeUTF(String.valueOf(pr));
    }

    public static class PageRankMapper extends Mapper<Text, Text, Text, Text> {
        protected void map(Text url, Text data, Context context) throws IOException, InterruptedException {
            // In:
            //          PR               out links
            // url1     3	    ###	    url2	url4

            // Out:
            // url2	    1.5
            // url4	    1.5
            // url1	    ###	    url2	url4

            Configuration conf = context.getConfiguration();
            String[] data_split = data.toString().split("###");
            String[] out = data_split[1].trim().split("\t");
            double prHanging = conf.getDouble("prHanging", 0);
            double pr;

            if (conf.get("isFirst").equals("1")) {
                // На первой итерации для всех документов ставим PR = 1/N.
                pr = 1. / nNodes;
            } else {
                pr = Double.parseDouble(data_split[0].trim());

                // Обновляем текущий PR c учетом PR висячих вершин полученный на предыдущей итерации
                pr += (1 - alpha) * prHanging / nNodes;
            }

            for (String item : out) {
                if (item.trim().isEmpty()) {
                    context.write(new Text("Hanging"), new Text(String.valueOf(pr)));
                } else {
                    context.write(new Text(item), new Text(String.valueOf(pr / out.length)));
                }

            }

            context.write(new Text(url), new Text("###\t" + String.join("\t", out)));
        }
    }

    public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text url, Iterable<Text> data, Context context) throws IOException, InterruptedException {
            double pr = 0;
            double prHanging = 0;
            String out = "";

            if (url.toString().equals("Hanging")) {
                for (Text item : data) {
                    prHanging += Double.parseDouble(item.toString());
                }
                // Записываем Pr висячих вершин
                Configuration conf = context.getConfiguration();
                prHangingWrite(conf, conf.get("pathHangingWrite"), prHanging);

            } else {
                for (Text item : data) {
                    char who = item.toString().trim().charAt(0);
                    if (who == '#') {
                        // Если это строка с исходящими ссылками
                        out = item.toString();
                    } else {
                        // Складываем Pr для текущего документа
                        pr +=  Double.parseDouble(item.toString());
                    }
                }
                // Добавляем фактор случайного перехода
                pr = alpha / nNodes + (1 - alpha) * pr;

                context.write(url, new Text(pr + "\t" + out));
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

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, InputPath);
        FileOutputFormat.setOutputPath(job, OutputPath);

        Configuration conf = job.getConfiguration();
        conf.set("isFirst", args[4]);
        conf.set("pathHangingWrite", args[3]);
        if (args[4].equals("1")) {
            // Если первая итерация
            conf.setDouble("prHanging", 0);
        } else {
            conf.setDouble("prHanging", prHangingRead(conf, args[2]));
        }

        // Mapper
        job.setMapperClass(PageRankMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Reduce
        job.setReducerClass(PageRankReducer.class);
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
            // Для каждой итерации своя папка output
            String OutputPathNew = Paths.get(OutputPath, folderName).toString();
            // В hangPrVal.txt в след итерации будем смотреть сумму PR всех висячих вершин
            int iBack = i - 1;
            String HangingPathRead = Paths.get(OutputPath, "iter" + iBack + "/hangPrVal.txt").toString();
            // Сюда запишем сумму PR всех висячих вершин на текущей итерации для будущих итераций
            String HangingPathWrite = Paths.get(OutputPath, folderName + "/hangPrVal.txt").toString();
            rc = ToolRunner.run(new PageRank(), new String[] {
                    InputPath,
                    OutputPathNew,
                    HangingPathRead,
                    HangingPathWrite,
                    isFirst
            });
            // Каждая новая итерация использует в качестве данных выходной результат предудущей
            InputPath = OutputPathNew + "/part-r-*";
            isFirst = "0";
        }
        System.exit(rc);
    }
}