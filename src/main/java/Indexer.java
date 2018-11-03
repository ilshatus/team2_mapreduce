import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Indexer {

    private final static String file_system_path = "hdfs://10.90.138.32:9000/user/team2/";

    public static class IdfMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        //google json parse
        private Gson g = new Gson();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //new document starts from new line
            StringTokenizer documents = new StringTokenizer(value.toString(), "\n");
            //map will contain the existence of the words
            Map<String, Boolean> wordExists = new HashMap<String, Boolean>();
            while (documents.hasMoreTokens()) {
                //parse json
                Document document = g.fromJson(documents.nextToken(), Document.class);
                //split into words
                String[] words = document.getText().toLowerCase()
                        .split("([ \n\t\r'\"!@#$%^&*()_\\-+={}\\[\\]|<>;:,./`~]|\\n)+");
                //for each word
                for (String word : words) {
                    boolean bad = false;
                    //check if word consists of letters
                    for (Character ch : word.toCharArray()) {
                        if (!(ch >= 'a' && ch <= 'z')) {
                            bad = true;
                            break;
                        }
                    }
                    if (bad) continue;
                    //if word does not exist, add it to hash map
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
            int sum = 0;
            //since mapper returns only ones for words existing in the document
            //we can sum up to find out the number of documents, containing this word
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class TfIdfMapper
            extends Mapper<Object, Text, Text, MapWritable> {
        private Gson g = new Gson();
        //this hash map will contain Idfs of documents
        private HashMap<String, Integer> wordsIdf;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if(wordsIdf == null){
                //Initialise HashMap
                wordsIdf = new HashMap<String, Integer>();

                //new configuration
                Configuration conf = context.getConfiguration();
                String idf_output = conf.get("idf_output");
                //Open file system
                FileSystem fileSystem = FileSystem.get(URI.create(file_system_path), conf);
                //iterator for files
                RemoteIterator<LocatedFileStatus> fileStatusListIterator = fileSystem.listFiles(
                        new Path(idf_output), true);
                //for all files in folder
                while (fileStatusListIterator.hasNext()) {
                    //open stream for file
                    FSDataInputStream stream = fileSystem.open(fileStatusListIterator.next().getPath());
                    Scanner scanner = new Scanner(stream);
                    //add number to map
                    while (scanner.hasNextLine()) {
                        String inputLine = scanner.nextLine();
                        String[] input = inputLine.split("[ \t]+");
                        wordsIdf.put(input[0], Integer.parseInt(input[1]));
                    }
                }
                fileSystem.close();
            }

            //split file by new line
            StringTokenizer documents = new StringTokenizer(value.toString(), "\n");
            //map will contain how much each word was in document
            Map<String, Integer> wordsCount = new HashMap<String, Integer>();
            while (documents.hasMoreTokens()) {
                //parse Json
                Document document = g.fromJson(documents.nextToken(), Document.class);
                //get all words
                String[] words = document.getText().toLowerCase()
                        .split("([ \n\t\r'\"!@#$%^&*()_\\-+={}\\[\\]|<>;:,./`~]|\\n)+");
                //for each word
                for (String word : words) {
                    boolean bad = false;
                    //check if word consists of letters
                    for (Character ch : word.toCharArray()) {
                        if (!(ch >= 'a' && ch <= 'z')) {
                            bad = true;
                            break;
                        }
                    }
                    if (bad) continue;
                    if (wordsCount.containsKey(word)) {
                        //if word is in the map increase its value by one
                        wordsCount.put(word, wordsCount.get(word) + 1);
                    } else {
                        //if word is not in the map, set the value to one
                        wordsCount.put(word, 1);
                    }
                }
                //divide tf by idf squared to avoid computing tf/idf for query, so that query tf is enough
                MapWritable tfIdf = new MapWritable();
                for (String word : wordsCount.keySet()) {
                    Integer tf = wordsCount.get(word);
                    Integer idf = wordsIdf.get(word);
                    tfIdf.put(new Text(word), new DoubleWritable(1D * tf / (idf * idf)));
                }
                context.write(new Text(document.getId()), tfIdf);
                wordsCount.clear();
            }
        }
    }

    private static void clear_folders(Configuration conf) throws URISyntaxException, IOException {
        //delete tf_idf_output folder if it exists
        String tf_idf_output = conf.get("tf_idf_output");
        FileSystem hdfs = FileSystem.get(new URI(file_system_path), conf);
        Path tfIdfFile = new Path(file_system_path + tf_idf_output);
        if (hdfs.exists(tfIdfFile)) {
            hdfs.delete(tfIdfFile, true);
            System.out.println("Removing existing " + tfIdfFile.getName() + " folder...");
        } else {
            System.out.println("Creating new " + tfIdfFile.getName() + " folder...");
        }

        //delete idf_output folder if it exists
        String idf_output = conf.get("idf_output");
        Path temp_idf_file = new Path(file_system_path + idf_output);
        if (hdfs.exists(temp_idf_file)) {
            hdfs.delete(temp_idf_file, true);
            System.out.println("Removing existing " + temp_idf_file.getName() + " folder...");
        } else {
            System.out.println("Creating new " + temp_idf_file.getName() + " folder...");
        }
        hdfs.close();
        System.out.println();
    }

    public static void main(String[] args) throws Exception {
        //read input
        System.out.println("\nINPUT:");
        String input_folder = args[1];
        System.out.println("\tinput folder: " + input_folder);
        String idf_output = args[2];
        System.out.println("\tidf output folder: " + idf_output);
        String tf_idf_output = args[3];
        System.out.println("\ttf idf output folder: " + tf_idf_output + "\n");

        //create configuration and save paths
        Configuration conf = new Configuration();
        conf.set("idf_output", idf_output);
        conf.set("tf_idf_output", tf_idf_output);
        clear_folders(conf);

        //create IDF counting job
        Job job = Job.getInstance(conf, "Word IDF");

        //configure job
        job.setJarByClass(Indexer.class);
        job.setMapperClass(IdfMapper.class);
        job.setCombinerClass(IdfReducer.class);
        job.setReducerClass(IdfReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(input_folder));
        FileOutputFormat.setOutputPath(job, new Path(idf_output));

        //wait for completion
        if (job.waitForCompletion(true)) {
            //create TF/IDF counting job
            Job job2 = Job.getInstance(conf, "Documents TF/IDF");

            //configure job
            job2.setJarByClass(Indexer.class);
            job2.setMapperClass(TfIdfMapper.class);
            job2.setNumReduceTasks(100);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(MapWritable.class);

            FileInputFormat.addInputPath(job2, new Path(input_folder));
            FileOutputFormat.setOutputPath(job2, new Path(tf_idf_output));

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        } else {
            System.exit(1);
        }
    }
}