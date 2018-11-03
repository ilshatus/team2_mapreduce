public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("Indexer")){
            if (args.length != 2 && args.length != 4) {
                System.out.println("\nUsage:  Indexer INPUT_FOLDER [IDF_OUTPUT_FOLDER TF_IDF_OUTPUT_FOLDER]");
                System.out.println("\tDefault for IDF_OUTPUT_FOLDER is 'idf_output'");
                System.out.println("\tDefault for TF_IDF_OUTPUT_FOLDER is 'tf_idf_output'\n");
                System.exit(1);
            } else if(args.length == 2){
                String temp[] = new String[] {args[0], args[1], "idf_output", "tf_idf_output"};
                Indexer.main(temp);
            } else {
                Indexer.main(args);
            }
        } else if (args.length > 0 && args[0].equals("Query")) {
            if (args.length != 3 && args.length != 5) {
                System.out.println("\nUsage:  Query [TF_IDF_INPUT_FOLDER RESULT_FOLDER] TOP QUERY_TEXT");
                System.out.println("\tDefault for TF_IDF_INPUT_FOLDER is 'tf_idf_output'");
                System.out.println("\tDefault for RESULT_FOLDER is 'result'\n");
                System.exit(1);
            } else if(args.length == 3){
                String temp[] = new String[] {args[0], "tf_idf_output", "result", args[1], args[2]};
                Query.main(temp);
            } else {
                Query.main(args);
            }
        } else {
            System.out.println("\nUsage:  Indexer INPUT_FOLDER [IDF_OUTPUT_FOLDER TF_IDF_OUTPUT_FOLDER]");
            System.out.println("\tDefault for IDF_OUTPUT_FOLDER is 'idf_output'");
            System.out.println("\tDefault for TF_IDF_OUTPUT_FOLDER is 'tf_idf_output'");
            System.out.println("\nUsage:  Query [TF_IDF_INPUT_FOLDER RESULT_FOLDER] TOP QUERY_TEXT");
            System.out.println("\tDefault for TF_IDF_INPUT_FOLDER is 'tf_idf_output'");
            System.out.println("\tDefault for RESULT_FOLDER is 'result'\n");
        }
    }
}
