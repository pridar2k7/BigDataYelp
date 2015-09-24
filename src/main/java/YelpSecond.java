import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

/**
 * Created by priyadarshini on 9/22/15.
 */
public class YelpSecond {
    public static class Map
            extends Mapper<LongWritable, Text, Text, Text> {
        private Text rating = new Text();
        private Text businessId = new Text(); // type of output key

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] mydata = value.toString().split("\\^");
            businessId.set(mydata[2]); // set word as each input keyword
            rating.set(mydata[3]);
            context.write(businessId, rating); // create a pair <keyword, 1>
        }
    }

    public static class Combine
            extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        //        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0; // initialize the sum for each keyword

            int count = 0;
            for (Text val : values) {
                sum += Double.parseDouble(val.toString());
                count++;
            }
            result.set(Integer.toString(sum) + "_" + Integer.toString(count));
            context.write(key, result); // create a pair <keyword, number of occurences>
        }
    }

    public static class Reduce
            extends Reducer<Text, Text, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalSum = 0;
            double totalCount = 0;
            for (Text val : values) {
                String[] concatenatedValue = val.toString().split("_");
                totalSum += Double.parseDouble(concatenatedValue[0]);
                totalCount += Double.parseDouble(concatenatedValue[1]);
            }
            double average = totalSum / totalCount;
            result.set(average);
            context.write(key, result);
        }

    }

    public static class SecondMap
            extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        TreeMap<Text, DoubleWritable> sampleText = new TreeMap<>();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String fullLine = value.toString();
            String[] splitedValue = fullLine.split("\\s+");
            sampleText.put(new Text(splitedValue[0]), new DoubleWritable(Double.parseDouble(splitedValue[1])));

        }


        @Override
        protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {

            java.util.Map<Text, DoubleWritable> sortedMap = sortByValues(sampleText);

            int counter = 0;
            for (Text key : sortedMap.keySet()) {
                if (counter++ == 10) {
                    break;
                }
                context.write(key, sortedMap.get(key));
            }
        }

        private static <K extends Comparable, V extends Comparable> java.util.Map<K, V> sortByValues(java.util.Map<K, V> map) {
            List<java.util.Map.Entry<K, V>> entries = new LinkedList<java.util.Map.Entry<K, V>>(map.entrySet());

            Collections.sort(entries, new Comparator<java.util.Map.Entry<K, V>>() {

                @Override
                public int compare(java.util.Map.Entry<K, V> o1, java.util.Map.Entry<K, V> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            //LinkedHashMap will keep the keys in the order they are inserted
            //which is currently sorted on natural ordering
            java.util.Map<K, V> sortedMap = new LinkedHashMap<K, V>();

            for (java.util.Map.Entry<K, V> entry : entries) {
                sortedMap.put(entry.getKey(), entry.getValue());
            }

            return sortedMap;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 4) {
            System.err.println("Usage: WordCount <business> <review> <user> <output>");
            System.exit(2);
        }

        Job job = new Job(conf, "yelpSecond");
        job.setJarByClass(YelpSecond.class);

        job.setReducerClass(Reduce.class);
        job.setCombinerClass(Combine.class);
        job.setMapperClass(Map.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        Job job1 = new Job(conf, "yelpSecond");
        job1.setJarByClass(YelpSecond.class);

        job1.setMapperClass(SecondMap.class);


        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setNumReduceTasks(0);

        Path temppath = new Path("/pxr143830/temp-dir-rev");
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, temppath);

        FileInputFormat.addInputPath(job1, temppath);
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[3]));
        job.waitForCompletion(true);
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }
}
