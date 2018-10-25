import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class IdfCounter {

    public static class IdfMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        //Google json parse
        private Gson g = new Gson();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //new document start from new line
            StringTokenizer documents = new StringTokenizer(value.toString(), "\n");
            //Map will contain existent of word
            Map<String, Boolean> wordExists = new HashMap<String, Boolean>();
            while (documents.hasMoreTokens()) {
                //parse json
                Document document = g.fromJson(documents.nextToken(), Document.class);
                //split to words
                String[] words = document.getText().toLowerCase()
                        .split("([ \n\t\r'\"!@#$%^&*()_\\-+={}\\[\\]|<>;:,.\\/`~]|\\n)+");
                //for each word
                for (String word : words) {
                    boolean bad = false;
                    //check if word consist of letters
                    for (Character ch : word.toCharArray()) {
                        if (!(ch >= 'a' && ch <= 'z')) {
                            bad = true;
                            break;
                        }
                    }
                    if (bad) continue;
                    //if word does not exist add to hash map
                    if (!wordExists.containsKey(word)) {
                        wordExists.put(word, true);
                        context.write(new Text(word), new IntWritable(1));
                    }
                }
                //clear hash map for new document
                wordExists.clear();
            }
        }
    }


    public static class IdfReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {

            Integer sum = 0;

            //Since mapper returns only one if it occurs in document
            //We can sum up to knw in how much document word occurs
            for (IntWritable val : values) {

                sum += val.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word IDF");

        job.setJarByClass(IdfCounter.class);

        job.setMapperClass(IdfMapper.class);
        job.setCombinerClass(IdfReducer.class);
        job.setReducerClass(IdfReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output_idf"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}