import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class Query {
    private final static String file_system_path = "hdfs://10.90.138.32:9000/user/team2/";

    private static class Pair {
        String id;
        double val;

        Pair(String id, Double val) {
            this.id = id;
            this.val = val;
        }
    }

    public static class QueryMapper
            extends Mapper<Object, Text, IntWritable, Text> {
        private HashMap<String, Integer> queryTf; // idf for words from query

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (queryTf == null) {
                //if the queryTf hashMap is not initialized
                queryTf = new HashMap<String, Integer>();
                //get the query_tf from the configuration
                String tfs[] = context.getConfiguration().get("query_tf").split("\n");
                //fill the hashMap
                for (String line : tfs) {
                    String input[] = line.split(" ");
                    queryTf.put(input[0], Integer.parseInt(input[1]));
                }
            }

            String str = value.toString();
            if (str.trim().length() == 0)
                return;
            //parse the document id
            int doc_id = Integer.parseInt(str.substring(0, str.indexOf('{')).trim());

            //parse the mapWritable
            String content = str.substring(str.indexOf('{') + 1, str.indexOf('}'));
            double res = 0.0;
            if (content.trim().length() == 0)
                return;

            //split the content into the word-tf/idf pairs
            String vals[] = content.split(",");
            for (String entry : vals) {
                String inputs[] = entry.split("=");
                String word = inputs[0].trim();
                Double val = Double.parseDouble(inputs[1]);
                //compute the document relevance
                if (queryTf.containsKey(word)) {
                    res += queryTf.get(word) * val;
                }
            }
            int split_value = 101; // value how we split data for the reducer of level1
            int hash = doc_id % split_value; // calculate hash, each documents with the same hash will be sorted and took 'top' on the next
            String result = doc_id + ";" + Double.toString(res);
            context.write(new IntWritable(hash), new Text(result));
        }
    }

    public static class QueryCombiner
            extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> value, Context context)
                throws IOException, InterruptedException {
            //get the top value from the configuration
            int top = Integer.parseInt(context.getConfiguration().get("top"));
            ArrayList<Pair> arrayList = new ArrayList<Pair>();
            //parse the data
            for (Text text : value) {
                String input[] = text.toString().split(";");
                String id = input[0];
                Double val = Double.parseDouble(input[1]);
                arrayList.add(new Pair(id, val));
            }
            //sort the data
            Collections.sort(arrayList, new Comparator<Pair>() {
                        @Override
                        public int compare(Pair p1, Pair p2) {
                            return -Double.compare(p1.val, p2.val);
                        }
                    }
            );
            //eliminate first top pairs
            for (int i = 0; i < top && i < arrayList.size(); ++i) {
                String result = arrayList.get(i).id + ";" + arrayList.get(i).val;
                context.write(new IntWritable(1), new Text(result));
            }
        }
    }

    public static class QueryReducer
            extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> value, Context context)
                throws IOException, InterruptedException {
            //get the top value from the configuration
            int top = Integer.parseInt(context.getConfiguration().get("top"));
            ArrayList<Pair> arrayList = new ArrayList<Pair>();
            //parse the data
            for (Text text : value) {
                String input[] = text.toString().split(";");
                String id = input[0];
                Double val = Double.parseDouble(input[1]);
                arrayList.add(new Pair(id, val));
            }
            //sort the data
            Collections.sort(arrayList, new Comparator<Pair>() {
                        @Override
                        public int compare(Pair p1, Pair p2) {
                            return -Double.compare(p1.val, p2.val);
                        }
                    }
            );
            //eliminate first top pairs and make the data look better
            for (int i = 0; i < top && i < arrayList.size(); ++i) {
                String resultValue = "Value: " + arrayList.get(i).val;
                String resultLink = "Link: " + "https://en.wikipedia.org/wiki?curid=" + arrayList.get(i).id;
                String resultId = "Id: " + arrayList.get(i).id;
                String result = resultId + " " + resultValue + " " + resultLink;
                context.write(new IntWritable(i + 1), new Text(result));
            }
        }
    }

    private static String process_tf(String text) {
        HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
        String[] words = text.toLowerCase()
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
            if (hashMap.containsKey(word)) {
                //if word is in the map increase its value by one
                hashMap.put(word, hashMap.get(word) + 1);
            } else {
                //if word is not in the map, set the value to one
                hashMap.put(word, 1);
            }
        }
        String result = "";
        for (Map.Entry<String, Integer> entry :
                hashMap.entrySet()) {
            result = result.concat(entry.getKey() + " " + entry.getValue() + "\n");
        }
        return result;
    }

    private static void clear_folders(Configuration conf) throws URISyntaxException, IOException {
        //delete result_folder folder if it exists
        FileSystem hdfs = FileSystem.get(new URI(file_system_path), conf);
        Path resultFile = new Path(file_system_path + conf.get("result_folder"));
        if (hdfs.exists(resultFile)) {
            hdfs.delete(resultFile, true);
            System.out.println("Removing existing " + resultFile.getName() + " folder...");
        } else {
            System.out.println("Creating new " + resultFile.getName() + " folder...");
        }
        hdfs.close();
        System.out.println();
    }

    public static void main(String[] args) throws Exception {
        //read input
        System.out.println("\nINPUT:");
        String tf_idf_output = args[1];
        System.out.println("\ttf idf folder: " + tf_idf_output);
        String result_folder = args[2];
        System.out.println("\tresult folder: " + result_folder);

        int top = 1;
        try {
            top = Integer.parseInt(args[3]);
            if (top <= 0) {
                System.out.println("Top must be greater than zero, got: " + top);
                System.exit(1);
            }
            System.out.println("\ttop: " + top);
        } catch (Exception ex) {
            System.out.println("Top must be an integer, got: " + args[3]);
            System.exit(1);
        }

        String query_text = args[4];
        System.out.println("\tquery: " + query_text + "\n");

        //create configuration and save variables
        Configuration conf = new Configuration();
        conf.set("tf_idf_output", tf_idf_output);
        conf.set("result_folder", result_folder);
        conf.set("top", String.valueOf(top));
        conf.set("query_tf", process_tf(query_text));
        clear_folders(conf);

        //create Query job
        Job job = Job.getInstance(conf, "Query");

        //configure job
        job.setJarByClass(Query.class);
        job.setMapperClass(QueryMapper.class);
        job.setCombinerClass(QueryCombiner.class);
        job.setReducerClass(QueryReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(tf_idf_output));
        FileOutputFormat.setOutputPath(job, new Path(result_folder));

        if (job.waitForCompletion(true)) {
            //Print result to the console
            FileSystem fileSystem = FileSystem.get(URI.create(file_system_path), conf);
            Path resultFile = new Path(file_system_path + result_folder + "/part-r-00000");
            Scanner scanner = new Scanner(fileSystem.open(resultFile));
            System.out.println("\nResult:");
            while (scanner.hasNextLine()) {
                System.out.println("\t" + scanner.nextLine());
            }
            fileSystem.close();
        } else {
            System.exit(1);
        }
    }
}
