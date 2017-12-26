
import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CheapestCondo {
    static class MyMapper extends Mapper<LongWritable,Text,NullWritable,Text>{
	public void map(LongWritable key,Text value,Context context)
	{   Text text=new Text();
		String str[]=value.toString().split(",");
		if(str[7].trim().equalsIgnoreCase("Condo"))
		{
			String val=str[1]+","+str[0]+","+str[9];
			text.set(val);
		try {
			context.write(NullWritable.get(),text);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		}
				}
    }
	static class MyReducer extends Reducer<NullWritable,Text,NullWritable,Text>{
       public void reduce(NullWritable Key,Iterable<Text> value,Context context) throws IOException, InterruptedException
	{
    	   
    	   Text text=new Text();
    	   int temp=0;
    	   int min=0;
    	   String city=null;
    	   String street=null;
    	   for(Text t:value)
    	 {
    		 String str[]=t.toString().split(",");
    	     temp=Integer.parseInt(str[2].trim());
    	     	if(min==0)
				{
					min=Integer.parseInt(str[2].trim());
				}
				if(temp<min)
				{
					min=temp;
					city=str[0];
					street=str[1];
				}
    	 }
    			 text.set(city+" "+street+" "+min);
    			 context.write(NullWritable.get(),text );
    	    
    		 }
    	 }
    	   
	           
	

	public static void main(String[] args) throws IOException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"Condo10k");
		job.setJarByClass(CheapestCondo.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job,new Path (args[0]));
        FileOutputFormat.setOutputPath(job, new Path (args[1]));
		
        try {
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		}
	
    }

