import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MapReduce {

    public static void main(String[] args) throws IOException {

        long overallTimeS= System.nanoTime(); //times from start to finish of program
        List<File> file_list = new LinkedList<File>();
        Map<String, String> input = new HashMap<String, String>();
        String str1 = "";

        if (args.length > 0) {
            for (int i = 1; i < args.length; i++) { //goes though each of the files inputted inputted from command line arguments
                File file;
                file = new File(args[i]);
                file_list.add(file); //creates list of inputted files
            }
        }

        for (File file : file_list) { //goes through the list of files
            BufferedReader br = new BufferedReader(new FileReader(file)); //reads in each file
            try {
                StringBuilder sb = new StringBuilder(); //new string builder
                String line = br.readLine(); //each line read in

                while (line != null) {
                    sb.append(line);
                    sb.append(System.lineSeparator());
                    line = br.readLine();
                }
                 str1 += sb.toString();
            } finally {
                br.close();
            }
        }

        str1  = str1.replaceAll("[\\-+.^:,!€£$\"'?]",""); //getting rid of unusual characters in the text file
        input.put("allFiles.txt", str1);


        // APPROACH #3: Distributed MapReduce with Thread Pooling
        {
            final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

            String strNum = args[0]; //first argument entered is the number of threads
            int threadsNum = Integer.parseInt(strNum); //parse the inputted string to an int
            ExecutorService executor = Executors.newFixedThreadPool(threadsNum); //creates thread pool with specified number of threads

            // MAP:

            long mapTimeS = System.nanoTime(); //starting time for map phase, same done for each phase
            final List<MappedItem> mappedItems = new LinkedList<MappedItem>();

            final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
                @Override
                public synchronized void mapDone(String file, List<MappedItem> results) {
                    mappedItems.addAll(results);
                }
            };

            List<Thread> mapCluster = new ArrayList<Thread>(input.size());

            Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
            while(inputIter.hasNext()) {
                Map.Entry<String, String> entry = inputIter.next();
                final String file = entry.getKey();
                final String contents = entry.getValue();

                executor.execute(() -> map(file, contents, mapCallback));  //Map executed asynchronously in thread pool
            }

            executor.shutdown(); //shut down executer
            while (!executor.isTerminated());

            long mapTimeE = System.nanoTime();

            // GROUP:

            long groupTimeS = System.nanoTime();

            Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

            Iterator<MappedItem> mappedIter = mappedItems.iterator();
            while(mappedIter.hasNext()) {
                MappedItem item = mappedIter.next();
                String word = item.getWord();
                String file = item.getFile();
                List<String> list = groupedItems.get(word);
                if (list == null) {
                    list = new LinkedList<String>();
                    groupedItems.put(word, list);
                }
                list.add(file);
            }
            long groupTimeE = System.nanoTime();

            // REDUCE:
            long reduceTimeS = System.nanoTime();

            final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
                @Override
                public synchronized void reduceDone(String k, Map<String, Integer> v) {
                    output.put(k, v);
                }
            };

            executor = Executors.newFixedThreadPool(threadsNum);

            Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
            while(groupedIter.hasNext()) {
                Map.Entry<String, List<String>> entry = groupedIter.next();
                final String word = entry.getKey();
                final List<String> list = entry.getValue();

                executor.execute(() -> reduce(word, list, reduceCallback));
            }

            executor.shutdown(); //shuts down the executor
            while (!executor.isTerminated());


//################################################################################################################################

        /*

        // APPROACH #2: MapReduce
        {
            Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

            // MAP:

            long mapTimeS = System.nanoTime(); //starting time for map phase, same done for each phase
            List<MapReduce.MappedItem> mappedItems = new LinkedList<MapReduce.MappedItem>();

            Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
            while(inputIter.hasNext()) {
                Map.Entry<String, String> entry = inputIter.next();
                String file = entry.getKey();
                String contents = entry.getValue();

                map(file, contents, mappedItems);
            }
            long mapTimeE = System.nanoTime();


            // GROUP:

            long groupTimeS = System.nanoTime();
            Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

            Iterator<MapReduce.MappedItem> mappedIter = mappedItems.iterator();
            while(mappedIter.hasNext()) {
                MapReduce.MappedItem item = mappedIter.next();
                String word = item.getWord();
                String file = item.getFile();
                List<String> list = groupedItems.get(word);
                if (list == null) {
                    list = new LinkedList<String>();
                    groupedItems.put(word, list);
                }
                list.add(file);
            }
            long groupTimeE = System.nanoTime();

            // REDUCE:

            long reduceTimeS = System.nanoTime();
            Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
            while(groupedIter.hasNext()) {
                Map.Entry<String, List<String>> entry = groupedIter.next();
                String word = entry.getKey();
                List<String> list = entry.getValue();

                reduce(word, list, output);
            }

         */


//################################################################################################################################


                    /*

        // APPROACH #3: Distributed MapReduce without thread pooling
        {
            final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

            // MAP:

            long mapTimeS = System.nanoTime(); //starting time for map phase, same done for each phase
            final List<MappedItem> mappedItems = new LinkedList<MappedItem>();

            final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
                @Override
                public synchronized void mapDone(String file, List<MappedItem> results) {
                    mappedItems.addAll(results);
                }
            };

            List<Thread> mapCluster = new ArrayList<Thread>(input.size());

            Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
            while(inputIter.hasNext()) {
                Map.Entry<String, String> entry = inputIter.next();
                final String file = entry.getKey();
                final String contents = entry.getValue();

                Thread t = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        map(file, contents, mapCallback);
                    }
                });
                mapCluster.add(t);
                t.start();
            }

            // wait for mapping phase to be over:
            for(Thread t : mapCluster) {
                try {
                    t.join();
                } catch(InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            long mapTimeE = System.nanoTime();

            // GROUP:

            long groupTimeS = System.nanoTime();

            Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

            Iterator<MappedItem> mappedIter = mappedItems.iterator();
            while(mappedIter.hasNext()) {
                MappedItem item = mappedIter.next();
                String word = item.getWord();
                String file = item.getFile();
                List<String> list = groupedItems.get(word);
                if (list == null) {
                    list = new LinkedList<String>();
                    groupedItems.put(word, list);
                }
                list.add(file);
            }
            long groupTimeE = System.nanoTime();

            // REDUCE:
            long reduceTimeS = System.nanoTime();

            final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
                @Override
                public synchronized void reduceDone(String k, Map<String, Integer> v) {
                    output.put(k, v);
                }
            };

            List<Thread> reduceCluster = new ArrayList<Thread>(groupedItems.size());

            Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
            while(groupedIter.hasNext()) {
                Map.Entry<String, List<String>> entry = groupedIter.next();
                final String word = entry.getKey();
                final List<String> list = entry.getValue();
                Thread t = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        reduce(word, list, reduceCallback);
                    }
                });
                reduceCluster.add(t);
                t.start();
            }

            // wait for reducing phase to be over:
            for(Thread t : reduceCluster) {
                try {
                    t.join();
                } catch(InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            //End of approach three without pooling
//################################################################################################################################

                     */

            long reduceTimeE = System.nanoTime(); //end time for reduce phase
            long overallTimeE = System.nanoTime(); //end time for the overall program

            long overallElapsed = (overallTimeE - overallTimeS); //Finding the elapsed time of the overall program
            long mapElapsed = (mapTimeE - mapTimeS);             //Finding the elapsed time for each phase below
            long groupElapsed = (groupTimeE - groupTimeS);
            long reduceElapsed = (reduceTimeE - reduceTimeS);
            long timeNew = (mapElapsed+groupElapsed+reduceElapsed); //added up each individual phase

            //System.out.println(output);

            System.out.println("**********************************************************");

            //Displays each elapsed time, divided by 1 billion to get in seconds
            System.out.println("\"Approach 3\" time (each phase added together): " + timeNew/1000000000.0);
            System.out.println("Map phase time: " + mapElapsed/1000000000.0);
            System.out.println("Group phase time: " +  groupElapsed/1000000000.0);
            System.out.println("Reduce phase time: " + reduceElapsed/1000000000.0);
            System.out.println("Overall program time (inc read in file): " + overallElapsed/1000000000.0);
        }
    }

    public static void map(String file, String contents, List<MappedItem> mappedItems) {
        String[] words = contents.trim().split("\\s+");
        for(String word: words) {
            mappedItems.add(new MappedItem(word, file));
        }
    }

    public static void reduce(String word, List<String> list, Map<String, Map<String, Integer>> output) {
        Map<String, Integer> reducedList = new HashMap<String, Integer>();
        for(String file: list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences.intValue() + 1);
            }
        }
        output.put(word, reducedList);
    }

    public static interface MapCallback<E, V> {

        public void mapDone(E key, List<V> values);
    }

    public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
        String[] words = contents.trim().split("\\s+");
        List<MappedItem> results = new ArrayList<MappedItem>(words.length);
        for(String word: words) {
            results.add(new MappedItem(word, file));
        }
        callback.mapDone(file, results);
    }

    public static interface ReduceCallback<E, K, V> {

        public void reduceDone(E e, Map<K,V> results);
    }

    public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {

        Map<String, Integer> reducedList = new HashMap<String, Integer>();
        for(String file: list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences.intValue() + 1);
            }
        }
        callback.reduceDone(word, reducedList);
    }

    private static class MappedItem {

        private final String word;
        private final String file;

        public MappedItem(String word, String file) {
            this.word = word;
            this.file = file;
        }

        public String getWord() {
            return word;
        }

        public String getFile() {
            return file;
        }

        @Override
        public String toString() {
            return "[\"" + word + "\",\"" + file + "\"]";
        }
    }
}
