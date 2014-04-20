import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class songs extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

	private final static DoubleWritable one = new DoubleWritable(1);
	private Text word = new Text();
	private String key2;
	private double val;

	public void configure(JobConf job) {
	}

	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	    String line = value.toString();
	    String[] lineArray =  line.split(",");
	    
	    if (Integer.parseInt(lineArray[4])>=2000)
	    		{
	    		key2 = lineArray[0]+" : "+lineArray[2];
	    		val = Double.parseDouble(lineArray[3]);
	    		word.set(key2);
	    		output.collect(word, new DoubleWritable(val));

	    		}
	    
	}
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	    
		double outv=values.next().get();
	    output.collect(key, new DoubleWritable(outv));
	}
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), songs.class);
	conf.setJobName("songs");

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
	int res = ToolRunner.run(new Configuration(), new songs(), args);
	System.exit(res);
    }
}