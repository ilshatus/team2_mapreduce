public class Main {
    public static void main(String[] args) throws Exception {
        if (args[0].equals("Indexer")){
            Indexer.main(args);
        }else if (args[0].equals("Query")) {
                Query.main(args);

        }else{
            System.out.println("Unknown class to execute");
        }
    }
}
