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
    private final static String tfPath = "hdfs://10.90.138.32:9000/user/team2/query_tf";

    private final static String topPath = "hdfs://10.90.138.32:9000/user/team2/top";

    private static String tf_idf = "tf_idf";
    private static String temp_idf = "temp_idf_output";
    private static String result_folder = "result";

    public static class QueryMapper
            extends Mapper<Object, Text, IntWritable, Text> {
        private HashMap<String, Integer> wordsIdf; // idfs of documents
        private HashMap<String, Double> queryTfIdf; // idf for words from query

        //Default constructor
        public QueryMapper() throws IOException {
            //Initialise HashMap
            wordsIdf = new HashMap<String, Integer>();
            //new configuration
            Configuration configuration = new Configuration();
            //Open file system
            FileSystem fileSystem = FileSystem.get(URI.create(file_system_path), configuration);
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

            queryTfIdf = new HashMap<String, Double>();
            Path hdfsPath = new Path(tfPath);
            try {
                BufferedReader bfr = new BufferedReader(new InputStreamReader(fileSystem.open(hdfsPath)));
                String str;
                while ((str = bfr.readLine()) != null) {
                    String input[] = str.split(" ");
                    if (wordsIdf.containsKey(input[0])) {
                        queryTfIdf.put(input[0], 1D * Integer.parseInt(input[1]) / wordsIdf.get(input[0]));
                    }
                }
            } catch (IOException ex) {
                System.out.println("QueryMapper - could not read from HDFS\n");
            }

            fileSystem.close();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            if(str.trim().length() == 0)
                return;
            int doc_id = Integer.parseInt(str.substring(0, str.indexOf('{')).trim());
            String content = str.substring(str.indexOf('{') + 1, str.indexOf('}'));
            double res = 0.0;
            if(content.trim().length() == 0)
                return;

            String vals[] = content.split(",");
            for (String entry : vals){
                try{
                String inputs[] = entry.split("=");
                String word = inputs[0].trim();
                Double val = Double.parseDouble(inputs[1]);
                if(queryTfIdf.containsKey(word)){
                    res+=queryTfIdf.get(word) * val;
                }}
                catch (Exception e){
                    throw new IOException("err:"+doc_id + " " + content);
                }
            }
            int rand = doc_id % 3;
            String result = doc_id + ";" + Double.toString(res);
            context.write(new IntWritable(rand), new Text(result));
        }
    }

    public static class QueryReducer
            extends Reducer<IntWritable, Text, IntWritable, Text> {
        private static class Pair {
            public String id;
            public double val;

            Pair(String id, Double val) {
                this.id = id;
                this.val = val;
            }
        }

        private int top = 0;

        QueryReducer() throws Exception{

            Configuration configuration = new Configuration();
            //Open file system
            FileSystem fileSystem = FileSystem.get(URI.create(file_system_path), configuration);
            Path hdfsPath = new Path(topPath);
            try {
                BufferedReader bfr = new BufferedReader(new InputStreamReader(fileSystem.open(hdfsPath)));
                top = Integer.parseInt(bfr.readLine());
            } catch (IOException ex) {
                System.out.println("QueryReducer - could not read from HDFS\n");
            }
        }

        public void reduce(IntWritable key, Iterable<Text> value, Context context)
                throws IOException, InterruptedException {
            ArrayList<Pair> arrayList = new ArrayList<Pair>();
            for (Text text : value) {
                String input[] = text.toString().split(";");
                String id = input[0];
                Double val = Double.parseDouble(input[1]);
                arrayList.add(new Pair(id, val));
            }
            Collections.sort(arrayList, new Comparator<Pair>() {
                        @Override
                        public int compare(Pair p1, Pair p2) {
                            return -Double.compare(p1.val, p2.val);
                        }
                    }
            );
            for(int i = 0; i < top && i < arrayList.size(); ++i ){
                String result = arrayList.get(i).id + ";" + arrayList.get(i).val;
                context.write(new IntWritable(1), new Text(result));
            }
        }
    }

    private static void save_top(int top) {
        try {
            Configuration configuration = new Configuration();
            FileSystem hdfs = FileSystem.get(new URI(file_system_path), configuration);
            Path file = new Path(topPath);
            if (hdfs.exists(file)) {
                hdfs.delete(file, true);
            }
            OutputStream os = hdfs.create(file);
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
            br.write(String.valueOf(top));
            br.close();
            hdfs.close();
        } catch (Exception ex) {
            System.out.println("couldn't write top value to hdfs");
            System.out.println(ex.getMessage());

            System.exit(0);
        }

    }

    private static void save_tf(String text) {
        HashMap<String, Integer> hashMap = process_tf_of_query(text);
        try {
            Configuration configuration = new Configuration();
            FileSystem hdfs = FileSystem.get(new URI(file_system_path), configuration);
            Path file = new Path(tfPath);
            if (hdfs.exists(file)) {
                hdfs.delete(file, true);
            }
            OutputStream os = hdfs.create(file);
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
            for (Map.Entry<String, Integer> entry :
                    hashMap.entrySet()) {
                br.write(entry.getKey() + " " + entry.getValue() + "\n");
            }
            br.close();
            hdfs.close();
        } catch (Exception ex) {
            System.out.println("couldn't write query map to hdfs");
            System.out.println(ex.getMessage());

            System.exit(0);
        }
    }

    private static HashMap<String, Integer> process_tf_of_query(String text) {
        HashMap<String, Integer> hashMap = new HashMap<String, Integer>();

        String[] words = text.toLowerCase()
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
            if (!hashMap.containsKey(word)) {
                hashMap.put(word, 1); // put 1 for new word
            } else {
                hashMap.put(word, hashMap.get(word) + 1); // increase by one for old
            }
        }
        return hashMap;
    }

    public static void clear_folders(Configuration configuration) throws URISyntaxException, IOException {
        FileSystem hdfs = FileSystem.get(new URI(file_system_path), configuration);
        Path resultFile = new Path(file_system_path + result_folder);
        if (hdfs.exists(resultFile)) {
            System.out.println("There is " + resultFile.getName() + " folder, it will be removed");
            hdfs.delete(resultFile, true);
        } else {
            System.out.println("New folder will be created: " + resultFile.getName());
        }
        hdfs.close();
    }

    public static void main(String[] args) throws Exception {
        for(int i = 0; i < 6; i++){
            if(i < args.length)
                continue;
            System.out.println("\nNot enough arguments: " + i + " instead of 6");
            return;
        }

        temp_idf = args[1];
        System.out.println("idf: " + temp_idf);

        tf_idf = args[2];
        System.out.println("tf_idf: " + tf_idf);

        result_folder = args[3];
        System.out.println("result_folder: " + result_folder);


        int top;
        try {
            top = Integer.parseInt(args[4]);
            System.out.println("top: " + top);
        } catch (Exception ex) {
            System.out.println("top must be integer, got: " + args[4]);
            return;
        }

        String query_text = args[5];
        System.out.println("query: " + query_text);

        save_top(top);
        save_tf(query_text);

        Configuration conf = new Configuration();

        clear_folders(conf);
        Job job = Job.getInstance(conf, "Query ");

        job.setJarByClass(Query.class);
        job.setMapperClass(QueryMapper.class);
        job.setCombinerClass(QueryReducer.class);
        job.setReducerClass(QueryReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(tf_idf));
        FileOutputFormat.setOutputPath(job, new Path(result_folder));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
