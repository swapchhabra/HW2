import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;

public class nGramsv2F extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

	private Text word = new Text();
	private String wkey;
	private double val;

	public void configure(JobConf job) {
	}

	public static boolean isInteger(String str) {
		if (str == null) {
			return false;
		}
		int length = str.length();
		if (length == 0) {
			return false;
		}
		int i = 0;
		if (str.charAt(0) == '-') {
			if (length == 1) {
				return false;
			}
			i = 1;
		}
		for (; i < length; i++) {
			char c = str.charAt(i);
			if (c <= '/' || c >= ':') {
				return false;
			}
		}
		return true;
	}
	
	
	public static boolean isYear(String str) {
		
		int length = str.length();
		
		if (length != 4) {
			return false;
		}
		
		if (  str.substring(0,1).equals("1")  ) {
			return true;
		}

		if (  str.substring(0,1).equals("2")  ) 
		{
			if (  !str.substring(1,2).equals("0")  ){return false;} 
			if (  !(str.substring(2,3).equals("0") | str.substring(2,3).equals("1"))  ){return false;}
			if (  !(str.substring(3,4).equals("0") | str.substring(3,4).equals("1") | str.substring(3,4).equals("2") | str.substring(3,4).equals("3"))  ){return false;} 
			return true;
		}
		return false;		
	}
	
	
	
	
	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	    {String line = value.toString();
	    
	    String[] lineArray =  line.split("\t");
	       
	    if (lineArray[0].toLowerCase().contains("nu"))
	    	{ if (isInteger(lineArray[1]))	    		    		   		
	    		if (isYear(lineArray[1]))
	    		{
	    		{
	    			wkey = lineArray[1]+" nu";
	    			val = Double.parseDouble(lineArray[3]);
	    			word.set(wkey);
	    			output.collect(word, new DoubleWritable(val));
	    		}
	    		}
	    	else
	    	 	{
	    		  if (lineArray[1].toLowerCase().contains("nu"))	    		 		
	    		  		{ if (isInteger(lineArray[2]))
	    		  			
	    		    		if (isYear(lineArray[2]))
	    		    		{ 			
	    		  			{
	    		  				wkey = lineArray[2]+" nu";
	    		  				val = Double.parseDouble(lineArray[4]);
	    		  				word.set(wkey);
	    		  				output.collect(word, new DoubleWritable(val));
	    		  			}	    		 
	    		    		}
	    		    		}  
	    	 	}
	    	}
		
	    
	    if (lineArray[0].toLowerCase().contains("die"))
    	{ if (isInteger(lineArray[1]))	    		    		   		
    		if (isYear(lineArray[1]))
    		{
    		{
    			wkey = lineArray[1]+" die";
    			val = Double.parseDouble(lineArray[3]);
    			word.set(wkey);
    			output.collect(word, new DoubleWritable(val));
    		}
    		}
    	else
    	 	{
    		  if (lineArray[1].toLowerCase().contains("die"))	    		 		
    		  		{ if (isInteger(lineArray[2]))
    		  			
    		    		if (isYear(lineArray[2]))
    		    		{ 			
    		  			{
    		  				wkey = lineArray[2]+" die";
    		  				val = Double.parseDouble(lineArray[4]);
    		  				word.set(wkey);
    		  				output.collect(word, new DoubleWritable(val));
    		  			}	    		 
    		    		}
    		    		}  
    	 	}
    	}

	    if (lineArray[0].toLowerCase().contains("kla"))
    	{ if (isInteger(lineArray[1]))	    		    		   		
    		if (isYear(lineArray[1]))
    		{
    		{
    			wkey = lineArray[1]+" kla";
    			val = Double.parseDouble(lineArray[3]);
    			word.set(wkey);
    			output.collect(word, new DoubleWritable(val));
    		}
    		}
    	else
    	 	{
    		  if (lineArray[1].toLowerCase().contains("kla"))	    		 		
    		  		{ if (isInteger(lineArray[2]))
    		  			
    		    		if (isYear(lineArray[2]))
    		    		{ 			
    		  			{
    		  				wkey = lineArray[2]+" kla";
    		  				val = Double.parseDouble(lineArray[4]);
    		  				word.set(wkey);
    		  				output.collect(word, new DoubleWritable(val));
    		  			}	    		 
    		    		}
    		    		}  
    	 	}
    	}
	    }
    }
    }
    public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	    double sum = 0;
	    int count=0;
	    double ave=0;
	    while (values.hasNext()) {
		sum += values.next().get();
		count++;
	    }
	    ave=sum/count;
	    output.collect(key, new DoubleWritable(ave));
	}
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), nGramsv2F.class);
	conf.setJobName("nGramsv2F");

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(DoubleWritable.class);

	conf.setMapperClass(Map.class);
	conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new nGramsv2F(), args);
	System.exit(res);
    }
}
    
