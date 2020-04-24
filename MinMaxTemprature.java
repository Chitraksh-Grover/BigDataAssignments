import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class MinMaxTemprature {

	
	public static class CityMapper extends Mapper<Object, Text, Text, Text>{
		

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String minTime = null;
			String maxTime = null;
			float minTemp = Float.MAX_VALUE;
			float maxTemp = Float.MIN_VALUE;			
			String data = value.toString();
			String list[] = data.split("\t");
			String date = list[0];
			Text codeDate = new Text();
			codeDate.set(list[0]);
			for(int i=1;i<list.length;i=i+2)
			{
				String time = list[i];
				float temprature = Float.parseFloat(list[i+1]);
				if( temprature > maxTemp)
				{
					maxTemp = temprature;
					maxTime = time;
				}
				else if( temprature < minTemp)
				{
					minTemp = temprature;
					minTime = time;
				}
			}
			Text timeTemp = new Text();
			timeTemp.set(maxTime + "\tMaxTemp: " + maxTemp);
			context.write(codeDate,timeTemp);
			timeTemp.set(minTime + "\tMinTemp: " + minTemp);
			context.write(codeDate,timeTemp);
		}
	}

	public static class CityReducer extends Reducer<Text, Text, Text, Text> {
		MultipleOutputs<Text, Text> mos;
		
		@Override
    		public void setup(Context context) {
        		mos = new MultipleOutputs<Text,Text>(context);
    		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			Dictionary cityCode = new Hashtable();			
			cityCode.put("CA","California");
			cityCode.put("AUS","Austin");
			cityCode.put("BOS","Boston");
			cityCode.put("NJ","Newjersy");
			cityCode.put("BAL","Baltimore");
			cityCode.put("NY","Newyork");
			String[] s = key.toString().split("_");
			String code = s[0];
			Text date = new Text(s[1]);	
			String max = null;
			String min = null;
			int cnt = 0;		
			for (Text value : values) {
				if (cnt==0){
					max = value.toString();
				}
				else{
					min = value.toString();
				}
				cnt = cnt + 1;
			}
			if (min.substring(10,13).equals("Max")){
				String temp = max;
				max = min;
				min = temp;
			}
			Text report = new Text("Time: " + min + "\t" + "Time: " + max);
			String city = cityCode.get(code).toString();
			mos.write(city , date, report);	
		}

		@Override
    		protected void cleanup(Context context) throws IOException, InterruptedException {
        		mos.close();
    		}	
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
   		Path inputDir = new Path(args[0]);
   		Path outputDir = new Path(args[1]);
	
   		Configuration conf = new Configuration();
	
   		Job job = new Job(conf);
   		job.setJarByClass(MinMaxTemprature.class);
   		job.setJobName("Min/Max Temp Report");
	
   		job.setOutputKeyClass(Text.class);
   		job.setOutputValueClass(Text.class);
	
   		job.setMapperClass(CityMapper.class);
   		job.setReducerClass(CityReducer.class);
	
   		FileInputFormat.setInputPaths(job, inputDir);
   		FileOutputFormat.setOutputPath(job, outputDir);
	
   		MultipleOutputs.addNamedOutput(job, "California", TextOutputFormat.class, Text.class, Text.class);
   		MultipleOutputs.addNamedOutput(job, "Austin", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "Boston", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "Newjersy", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "Baltimore", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "Newyork", TextOutputFormat.class, Text.class, Text.class);
	
   		job.waitForCompletion(true);
	}
}
