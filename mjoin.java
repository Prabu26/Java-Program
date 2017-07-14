import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class mjoin 
{
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		
                
	        private TreeMap<Integer, Integer> map = new TreeMap<Integer, Integer>() ; 
	        private TreeMap<Integer, Integer> map1= new TreeMap<Integer, Integer>() ;
	        private TreeMap<Integer, Integer> map2= new TreeMap<Integer, Integer>() ;
	        private Text outputkey=new Text();
		private Text outputvalue=new Text();
                protected void setup(Context context) throws java.io.IOException,InterruptedException
		{
			super.setup(context);
			URI[] files=context.getCacheFiles();
			Path p=new Path(files[0]);
			Path p1= new Path(files[1]);
			if(p.getName().equals("D02.txt"))
			{
			BufferedReader reader= new BufferedReader(new FileReader(p.toString()));
			String line = reader.readLine();
			while(line!=null)
				{
					String[] tokens= line.split(";");
					String cus_id=tokens[1];
					String totalcost=tokens[7];
					map.put(cus_id,totalcost);
					line=reader.readLine();
				}
					reader.close();
			}
			if(p1.getName().equals("D11.txt"))
			{
				BufferedReader reader= new BufferedReader(new FileReader(p1.toString()));
				String line=reader.readLine();
				while(line!=null)
				{
					String[] tokens=line.split(";");
					String cus_id=tokens[1];
					String totalcost=tokens[7];
					map1.put(emp_id, emp_desig);
					line=reader.readLine();
				}
				reader.close();
			}
			if(p1.getName().equals("D12.txt"))
			{
				BufferedReader reader= new BufferedReader(new FileReader(p1.toString()));
				String line=reader.readLine();
				while(line!=null)
				{
					String[] tokens=line.split(";");
					String cus_id=tokens[1];
					String totalcost=tokens[7];
					map2.put(emp_id, emp_desig);
					line=reader.readLine();
				}
				reader.close();
			}
			if(map.isEmpty())
			{
				throw new IOException("Error : Unable To Load Salary Data");
			}
			if(map1.isEmpty())
			{
				throw new IOException("Error : Unable To Load Designation Data");
			}
		}
	

public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
{
			String row= value.toString();
			String[] tokens=row.split(";");
			String cus_id=tokens[1];
			String totalcost=tokens[7];
			String D02=map.get(cus_id,totalcost);
			String D11=map1.get(cus_id,totalcost);
			String D12=map2.get(cus_id,totalcost);
			string out=D02+""+D11+""+D12;
			outputkey.set(row);
			outputvalue.set(out);
			context.write(outputkey,outputvalue);
			}
		}
public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf=new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator"; ";");
		Job job=Job.getInstance(conf);
		job.setJarByClass(mjoin.class);
		job.setJobName("Map Side Join");
		job.setMapperClass(MyMapper.class);
		job.addCacheFile(new Path("D02.txt").toUri());
		job.addCacheFile(new Path("D11.txt").toUri());
		job.addCacheFile(new Path("D12.txt").toUri());
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[7]));
		
		job.waitForCompletion(true);
		
	}
}


