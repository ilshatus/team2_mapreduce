import com.google.gson.Gson;
import javafx.beans.binding.ObjectExpression;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;

import java.awt.image.RescaleOp;
import java.io.*;
import java.net.URI;
import java.sql.SQLOutput;
import java.util.*;

public class Query {
    private final static String file_system_path = "hdfs://10.90.138.32:9000/user/team2/";
    private final static String tfPath = "hdfs://10.90.138.32:9000/user/team2/query_tf";

    private final static String topPath = "hdfs://10.90.138.32:9000/user/team2/top";

    public static class QueryMapper
            extends Mapper<Text, MapWritable, IntWritable, Text> {
        private Gson g = new Gson();

        //This hash map will contain Idfs of documents
        private HashMap<String, Integer> wordsIdf;

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


            Path hdfsPath = new Path(tfPath);
            StringBuilder fileContent = new StringBuilder("");
            try {
                BufferedReader bfr = new BufferedReader(new InputStreamReader(fileSystem.open(hdfsPath)));
                String str;
                while ((str = bfr.readLine()) != null) {
                    String input[] = str.split(" ");
                    System.out.println("read from map " + str);

                    if (wordsIdf.containsKey(input[0])) {
                        queryTfIdf.put(input[0], 1D * Integer.parseInt(input[1]) / wordsIdf.get(input[0]));
                    }

                }
            } catch (IOException ex) {
                System.out.println("----------Could not read from HDFS---------\n");
            }

            fileSystem.close();
        }

        public void map(Text key, MapWritable value, Context context) throws IOException, InterruptedException {
            double res = 0.0;


            for (Map.Entry<String, Double> entry : queryTfIdf.entrySet()) {
                Text word = new Text(entry.getKey());
                if (value.containsKey(word)) {
                    Double val = ((DoubleWritable) value.get(word)).get();
                    res += val * entry.getValue();
                }
            }


            int rand = key.hashCode() % 3;
            String result = key + ";" + Double.toString(res);
            context.write(new IntWritable(rand), new Text(result));
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
            if (hashMap.containsKey(word)) {
                hashMap.put(word, 1); // put 1 for new word
            } else {
                hashMap.put(word, hashMap.get(word) + 1); // increase by one for old
            }
        }
        return hashMap;
    }

    public static class QueryReducer
            extends Reducer<IntWritable, Text, IntWritable, Text > {

        private int top = 0;

        QueryReducer() throws Exception{
            Configuration configuration = new Configuration();
            //Open file system
            FileSystem fileSystem = FileSystem.get(URI.create(file_system_path), configuration);
            StringBuilder fileContent = new StringBuilder("");
            Path hdfsPath = new Path(topPath);
            try {
                BufferedReader bfr = new BufferedReader(new InputStreamReader(fileSystem.open(hdfsPath)));
                String str;
                top = Integer.parseInt(bfr.readLine());
                System.out.println("top equal : " + top);
            } catch (IOException ex) {
                System.out.println("----------Could not read from HDFS---------\n");
            }

        }

        private static class Pair {
            public String id;
            public double val;

            Pair(String id, Double val) {
                this.id = id;
                this.val = val;
            }

        }

        public void reduce(IntWritable key, Iterable<Text> value,
                           Context context
        ) throws IOException, InterruptedException {
            ArrayList<Pair> arrayList = new ArrayList<Pair>();
            for (Text text : value) {
                String input[] = text.toString().split(";");
                String id = input[0];
                Double val = Double.parseDouble(input[1]);
                arrayList.add(new Pair(id, val));
            }
            if (arrayList.size() > top) {
                Collections.sort(arrayList, new Comparator<Pair>() {
                            @Override
                            public int compare(Pair p1, Pair p2) {
                                return -Double.compare(p1.val, p2.val);
                            }
                        }
                );
            }
            for(int i = 0; i < top && i < arrayList.size(); ++i ){
                String result = arrayList.get(i).id + ";" + arrayList.get(i).val;
                context.write(new IntWritable(1), new Text(result));
            }
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
            System.out.println("couldnt write query map to hdfs");
            System.out.println(ex.getMessage());

            System.exit(0);
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
            br.write(top);
            br.close();
            hdfs.close();
        } catch (Exception ex) {
            System.out.println("couldnt write top value  to hdfs");
            System.out.println(ex.getMessage());

            System.exit(0);
        }

    }

    public static void main(String[] args) throws Exception {
        int top;
        String query_text ;
        try {
            top = Integer.parseInt(args[4]);
        } catch (Exception ex) {
            System.out.println("specify the number of articles for ranking");
            return;
        }

        try {
            query_text = args[5];
        } catch (Exception ex) {
            System.out.println("specify the query text");
            return;
        }

        save_tf(query_text);
        save_top(top);


        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Query ");

        job.setJarByClass(Query.class);
        job.setMapperClass(QueryMapper.class);
        job.setCombinerClass(QueryReducer.class);
        job.setReducerClass(QueryReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("output_tf_idf"));
        FileOutputFormat.setOutputPath(job, new Path("Result"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
