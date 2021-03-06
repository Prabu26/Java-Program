import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
	public class AreaCustomer
	{
		public static class AreaCustomerMapper extends
		Mapper<LongWritable, Text, Text,Text > 
		{
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
			{
				String[] record = value.toString().split(";");
				context.write(new Text(record[1]), new Text(record[3]+","+record[8]));
			}
		}

		public static class AreaCustomerPartitioner extends
		   Partitioner < Text, Text >
		   {
		      @Override
		      public int getPartition(Text key, Text value, int numReduceTasks)
		      {
		    	  String[] str = value.toString().split(",");
			      if(str[0].trim().equals("A"))
			    	  return 0;
			      else if(str[0].trim().equals("B"))
			    	  return 1;
			      else if(str[0].trim().equals("C"))
			    	  return 2;
			      else if(str[0].trim().equals("D"))
			    	  return 3;
			      else if(str[0].trim().equals("E"))
			    	  return 4;
			      else if(str[0].trim().equals("F"))
			    	  return 5;
			      else if(str[0].trim().equals("G"))
			    	  return 6;
			      else if(str[0].trim().equals("H"))
			    	  return 7;
			       return 7;
			        
		      }
		      
		   }
		public static class AreaCustomerReducer extends Reducer <Text, Text, NullWritable, Text> {
			 private TreeMap<LongWritable, Text> AreaCustomerMap = new TreeMap<LongWritable, Text>();
			long SUMsales=0;String answer=null;int count=0;
			public void reduce(Text key, Iterable <Text> value, Context context) throws IOException, InterruptedException
			{
				for(Text val:value)
				 {
					String[] record = val.toString().split(",");
					SUMsales=SUMsales+Long.parseLong(record[1]);
					answer=key.toString()+","+Long.toString(SUMsales)+","+record[0];
				 }
				AreaCustomerMap.put(new LongWritable(SUMsales),new Text(answer));
				if (AreaCustomerMap.size()>5)
					AreaCustomerMap.remove(AreaCustomerMap.firstKey());
			}
				protected  void cleanup (Context context) throws IOException, InterruptedException  
				{
					for (Text t : AreaCustomerMap.descendingMap().values()) 
					context.write(NullWritable.get(), t);
				}
		}
		
		public static void main(String args[]) throws  IOException, ClassNotFoundException, InterruptedException
		{
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Area wise Customer");
		    job.setJarByClass(AreaCustomer.class);
		    job.setMapperClass(AreaCustomerMapper.class);
		    job.setPartitionerClass(AreaCustomerPartitioner.class);
		    job.setReducerClass(AreaCustomerReducer.class);
		    job.setNumReduceTasks(9);
		    
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    
		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}
