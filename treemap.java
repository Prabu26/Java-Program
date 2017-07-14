import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;

public class TopK extends Configured implements Tool {

 

  public static class MapClass
                      extends Mapper<LongWritable, Text, Text, Text> {

     private TreeMap<Integer, Text> TopKMap = new TreeMap<Integer, Text>() ;

     public void map(LongWritable key, Text value, Context context)
                     throws IOException, InterruptedException {

            String[] d = value.toString().split(";") ;
            int value = d[1].split(";");

            TopKMap.put(new Integer(value), new Text(d[7]));
	    

            
     } //~map

     protected void cleanup(Context context) throws IOException, InterruptedException {
       for( int i=0;i<n;i++)
	    {
             if(max<a[i])
             {
             max=a[i]];
              }
     } //~cleanup

  } //~MapClass


  public static class ReduceClass extends Reducer<Text, Text, Text, Text >

     private static final TreeMap<Text, Text> TopKMap = new TreeMap <Text, Text>(); 

     @Override
     public void reduce (Text key, Iterable<Text> values, Context context)
                 throws IOException, InterruptedException {

            for (Text value : values) {
                TopKMap.put(new Text(key), new Text(value));
                if (TopKMap.size() > N) {
                   TopKMap.remove(TopKMap.firstKey()) ;
                }
            }


     } //~reduce

     @Override
     protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Text k : TopKMap.keySet()) {
                 context.write(k, TopKMap.get(k));
            } 
     } //~cleanup

  } // ReduceClass

  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    Job job = new Job(conf, "TopK");
    job.setJarByClass(TopK.class);

    Path in = new Path(args[0]);
    Path out = new Path(args[1]);
    FileInputFormat.setInputPaths(job, in);
    FileOutputFormat.setOutputPath(job, out);

    job.setMapperClass(MapClass.class);
    job.setReducerClass(ReduceClass.class);
    job.setNumReduceTasks(1);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    System.exit(job.waitForCompletion(true)?0:1);

    return 0;
    
  } //~run

  public static void main(String[] args) throws Exception {
     int res = ToolRunner.run(new Configuration(), new TopK(), args);

     System.exit(res);
  } //~main

} 
