import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by priyadarshini on 9/22/15.
 */
public class Yelp {
    public static class Map
            extends Mapper<LongWritable, Text, Text, Text> {
//        private final static IntWritable one = new IntWritable(1);
        private Text address = new Text(); // type of output key
        private Text businessId = new Text(); // type of output key
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] mydata = value.toString().split("\\^");
//            for (String data : mydata) {
//                word.set(data); // set word as each input keyword
//                context.write(word, one); // create a pair <keyword, 1>
//            }
            if(mydata[1].contains("Palo Alto"))
            {
                address.set(mydata[1]);
                address.set(mydata[0]);
                context.write(address, businessId);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 4) {
            System.err.println("Usage: WordCount <business> <review> <user> <output>");
            System.exit(2);
        }
// create a job with name "yelp"
        Job job = new Job (conf, "yelp");
        job.setJarByClass(Yelp.class);

        job.setMapperClass(Map.class);
//        job.setReducerClass(Reduce.class);
//        job.setCombinerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
