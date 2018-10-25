import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;

public class TfIdfCounter {
    public static class TfIdfMapper
            extends Mapper<Object, Text, Text, MapWritable> {
        private Gson g = new Gson();

        private final String idfPath = "hdfs://10.90.138.32:9000/user/team2/";
        //This hash map will contain Idfs of documents
        private HashMap<String, Integer> wordsIdf;

        //Default constructor
        public TfIdfMapper() throws IOException {
            //Initialise HashMap
            wordsIdf = new HashMap<String, Integer>();

            //new configuration
            Configuration configuration = new Configuration();
            //Open file system
            FileSystem fileSystem = FileSystem.get(URI.create(idfPath), configuration);
            //iterator for files
            RemoteIterator<LocatedFileStatus> fileStatusListIterator = fileSystem.listFiles(
                    new Path("output_idf/"), true);
            //for all files in folder
            while (fileStatusListIterator.hasNext()) {
                //open stream for file
                FSDataInputStream stream = fileSystem.open(fileStatusListIterator.next().getPath());
                Scanner scanner = new Scanner(stream);
                //Add number to map
                while (scanner.hasNextLine()) {
                    String inputLine = scanner.nextLine();
                    String[] input = inputLine.split("[ \t]+");
                    wordsIdf.put(input[0], Integer.parseInt(input[1]));
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //split file by new line
            StringTokenizer documents = new StringTokenizer(value.toString(), "\n");
            //map will contain how much each word was in document
            Map<String, Integer> wordsCount = new HashMap<String, Integer>();
            while (documents.hasMoreTokens()) {
                //parse Json
                Document document = g.fromJson(documents.nextToken(), Document.class);
                //get all words
                String[] words = document.getText().toLowerCase()
                        .split("([ \n\t\r'\"!@#$%^&*()_\\-+={}\\[\\]|<>;:,.\\/`~]|\\n)+");
                //for each word
                for (String word : words) {
                    boolean bad = false;
                    //check word if words does not consist of letters skip this word
                    for (Character ch : word.toCharArray()) {
                        if (!(ch >= 'a' && ch <= 'z')) {
                            bad = true;
                            break;
                        }
                    }
                    if (bad) continue;
                    //if word is in map increase value by one
                    if (wordsCount.containsKey(word)) {
                        wordsCount.put(word, wordsCount.get(word) + 1);
                    } else {
                        //if word is not in map set value one and the word
                        wordsCount.put(word, 1);
                    }
                }
                //divide tf by idf
                MapWritable tfIdf = new MapWritable();
                for (String word : wordsCount.keySet()) {
                    Integer tf = wordsCount.get(word);
                    Integer idf = wordsIdf.get(word);
                    tfIdf.put(new Text(word), new DoubleWritable(1D * tf / idf));
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
