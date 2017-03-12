import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.path;

import org.apache.hadoop.io.IntWriteable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DistinctWord {
	//this is a mapper class that take from file as a object
	// sends text in a line and 1 as a count
	public static class TokenizerMapper
		extends Mapper<Object, text, text, IntWritable>{
//this is declaring variables for the word and its count
		private final static IntWritable one= new IntWritable(1);
		private Text word =new Text();
		
// mapper method is creating with logic to find the word
		public void map(Object k,Text v, Context C )
			throws IOException, InterruptedException{
				StringTokenizer str = new StringTokenizer(v.toString());
				
				while(str.hasMoreTokenizer()){
					word.set(str.nextToken());
					c.write(word,one);
				}
			}
		}
		public static class DistWordReducer
			extends Reducer<text,Intwritable, text, IntWritable> {
			
			private IntWritable result = new IntWritable(0);
			
			public void reduce(Text k, Iterable<IntWriable> v, Context c)
				throws IOException, InterruptedException
			{
				//int sum =0;
				//for (IntWriable i: v)
				//	sum+=i.get();
				//result.set(sum);
				c.write(k,results);
			}

		}

		public static void main(String args[]) throws Exception
		{
			Configuration cf = new Configuration();
			Job j = new Job(cf, "dist word");
			j.setJarByClass(DistinctWord.class);
			j.setMapperClass(DistinctWord.class);
			j.setCombinerClass(DistinctWord.class);
			j.setReducerClass(DistinctWord.class);
			j.setOutputKeyClass(Text.class);
			//j.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(j, new Path(args[0]));
			FileOutputFormat.addOutputPath(j,new Path(args[1]));
			System.exit(j.waitForCompletion(true) ? 0 : 1);

		}
}
