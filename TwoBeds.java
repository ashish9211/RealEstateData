//separate list of residential apartments with more than 2 beds. 
//Also include columns in following order City,Baths,Sq_feet,Price,flat_type,Beds respectively.
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



public class TwoBeds {
    static class MyMapper extends Mapper<LongWritable,Text,NullWritable,Text>{
	public void map(LongWritable key,Text value,Context context)
	{   Text text=new Text();
		String str[]=value.toString().split(",");
		if( Integer.parseInt(str[4].trim())>2)
		{
			String val=str[1]+","+str[5]+","+str[6]+","+str[9]+","+str[7]+","+str[4];
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
    	 for(Text t:value)
    	 {
    		 String str[]=t.toString().split(",");
    		
    			 text.set(str[0]+" "+str[1]+" "+str[2]+" "+str[3]+" "+str[4]+" "+str[5]);
    			 context.write(NullWritable.get(),text );
    	
    		 }
    	 }
    	   
	           
	}

	public static void main(String[] args) throws IOException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"Condo10k");
		job.setJarByClass(TwoBeds.class);
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
