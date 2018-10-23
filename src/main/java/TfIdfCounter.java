import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;

public class TfIdfCounter {
    public static class TfIdfMapper
            extends Mapper<Object, Text, Text, MapWritable> {
        private Gson g = new Gson();

        private HashMap<String, Integer> wordsIdf;

        public TfIdfMapper() throws IOException {
            wordsIdf = new HashMap<String, Integer>();

            Configuration configuration = new Configuration();
            configuration.addResource(new Path("/hadoop/etc/hadoop/core-site.xml"));
            configuration.addResource(new Path("/hadoop/etc/hadoop/hdfs-site.xml"));

            FileSystem fileSystem = FileSystem.get(configuration);
            FSDataInputStream stream = fileSystem.open(new Path("output_idf"));
            Scanner scanner = new Scanner(stream);
            while (scanner.hasNextLine()) {
                String inputLine = scanner.nextLine();
                String[] input = inputLine.split("[ \t]+");
                wordsIdf.put(input[0], Integer.parseInt(input[1]));
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer documents = new StringTokenizer(value.toString(), "\n");
            Map<String, Integer> wordsCount = new HashMap<String, Integer>();
            while (documents.hasMoreTokens()) {
                Document document = g.fromJson(documents.nextToken(), Document.class);
                String[] words = document.getText().toLowerCase()
                        .split("([ \n\t\r'\"!@#$%^&*()_\\-+={}\\[\\]|<>;:,.\\/`~]|\\n)+");
                for (String word : words) {
                    boolean bad = false;
                    for (Character ch : word.toCharArray()) {
                        if (!(ch >= 'a' && ch <= 'z')) {
                            bad = true;
                            break;
                        }
                    }
                    if (bad) continue;
                    if (wordsCount.containsKey(word)) {
                        wordsCount.put(word, wordsCount.get(word) + 1);
                    } else {
                        wordsCount.put(word, 1);
                    }
                }
                MapWritable tfIdf = new MapWritable();
                for (String word : wordsCount.keySet()) {
                    Integer tf = wordsCount.get(word);
                    Integer idf = wordsIdf.get(word);
                    tfIdf.put(new Text(word), new DoubleWritable(tf / idf));
                }
                context.write(new Text(document.getId()), tfIdf);
                wordsCount.clear();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Documents TF/IDF");

        job.setJarByClass(TfIdfCounter.class);

        job.setMapperClass(TfIdfMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output_tf_idf"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
