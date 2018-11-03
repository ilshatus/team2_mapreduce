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
    private static String temp_idf = "temp_idf_output";
    private static String tf_idf_output = "tf_idf";
    private static String input_folder = "EnWikiAA";

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
                    new Path(temp_idf), true);
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
            fileSystem.close();
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
    public static void clear_folders(Configuration configuration) throws URISyntaxException, IOException {
        FileSystem hdfs = FileSystem.get(new URI(file_system_path), configuration);
        Path tfIdfFile = new Path(file_system_path+tf_idf_output);
        if (hdfs.exists(tfIdfFile)) {
            System.out.println("There is " + tfIdfFile.getName() + " folder, it will be removed");
            hdfs.delete(tfIdfFile, true);
        }else{
            System.out.println("New folder will be created: "+ tfIdfFile.getName());
        }
        Path temp_idf_file = new Path(file_system_path+temp_idf);
        if (hdfs.exists(temp_idf_file)) {
            System.out.println("There is " + temp_idf_file.getName() + " folder, it will be removed");
            hdfs.delete(temp_idf_file, true);
        }else{
            System.out.println("New folder will be created: "+ temp_idf_file.getName());
        }
        hdfs.close();
    }
    public static void main(String[] args) throws Exception {
        if (args.length<2){
            System.out.println("There is no enough arguments are passed");
            System.exit(1);
        }
        // if there is no less than 3 arguments keep temp_idf path default
        if(args.length>2) {
            temp_idf =args[2];
        }
        input_folder = args[0];
        tf_idf_output =args[1];

        Configuration conf = new Configuration();
        clear_folders(conf);
        Job job = Job.getInstance(conf, "Word IDF");

        job.setJarByClass(Indexer.class);

        job.setMapperClass(IdfMapper.class);
        job.setCombinerClass(IdfReducer.class);
        job.setReducerClass(IdfReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(input_folder));
        FileOutputFormat.setOutputPath(job, new Path(temp_idf));

        if (job.waitForCompletion(true)){


            Job job2 = Job.getInstance(conf, "Documents TF/IDF");

            job2.setJarByClass(TfIdfCounter.class);

            job2.setMapperClass(TfIdfMapper.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(MapWritable.class);
            job2.setNumReduceTasks(5);
            FileInputFormat.addInputPath(job2, new Path(input_folder));
            FileOutputFormat.setOutputPath(job2, new Path(tf_idf_output));

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }else{
            System.exit(1);
        }
    }
}