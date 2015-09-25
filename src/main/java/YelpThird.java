import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class YelpThird {

    public static class ReviewsByAreaMapper extends Mapper<LongWritable, Text, Text, Text> {

        HashMap<String, String> businessLocationMap = new HashMap<>();
        //Distributed cache does not work on the cluster, pls use thiscode for the setup phase instead.
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            //read data to memory on the mapper.
            List myCenterList = new ArrayList<>();
            Configuration conf = context.getConfiguration();
            String myfilepath = conf.get("myFilePath");
            //e.g /user/hue/input/
            Path part=new Path("hdfs://cshadoop1/pxr143830/business");//Location of file in HDFS


            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
            FileStatus[] fss = fs.listStatus(part);
            context.write(new Text("read line"), new Text(fss[0].toString()));

            for (FileStatus status : fss) {
                Path pt = status.getPath();

                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line;
                line=br.readLine();
                while (line != null){
                    System.out.println(line);
                    //do what you want with the line read
                    String[] values = line.split("\\^");
//                    context.write(new Text("read line"), new Text(String.valueOf(values.length)+"__"+values[0]));
                    businessLocationMap.put(values[0],values[1]);
                    line=br.readLine();
                    System.err.println(line);
                }

            }
        }


        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            String[] input = value.toString().trim().split("\\^");
            try{
//                context.write(new Text(input[1].trim()), new Text(businessLocationMap.get(input[2].trim())));
                if(businessLocationMap.get(input[2].trim()).contains("Stanford,")){
                    context.write(new Text(input[1].trim()), new Text(input[3].trim()));
                }
            }
            catch(NullPointerException e){
                //don't emit anything
                System.err.println(e);
            }
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException{
        Configuration conf=new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length!=3){
            System.err.println("Error! Insufficient arguments. Provide arguments <Input file path:businesses> <input directory:reviews> <Output directory>");
            System.exit(2);
        }

        Job job=new Job(conf, "ReviewsByArea");
        job.setJarByClass(YelpThird.class);
        job.setMapperClass(ReviewsByAreaMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        conf.set("myFilePath", otherArgs[0]);
//        final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
//        DistributedCache.addCacheFile(new URI(otherArgs[0]), conf);
//        job.addCacheFile(new URI(NAME_NODE+ "/" +otherArgs[0]));
//        job.addCacheFile(new URI(NAME_NODE+ "/" +otherArgs[0]));
//        job.addCacheFile(new URI(otherArgs[0]));
        FileInputFormat.setInputPaths(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}