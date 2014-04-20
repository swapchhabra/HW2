import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;







public class songsMD2 extends Configured implements Tool {

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
	    
	    		key2 = lineArray[2];
	    		val = Double.parseDouble(lineArray[3]);
	    		word.set(key2);
	    		output.collect(word, new DoubleWritable(val));

	    
	}
    }

//    public interface MyPartitioner<K, V> extends JobConfigurable  {
 //   	int getPartition(K key, V value, int indexOfReducer);
    	
//    	int ior = indexOfReducer;
//    	return ((key.toString().charAt(0)) % ior);
    	
    	
 //   }
    
    public static class Partition extends MapReduceBase implements Partitioner<Text, DoubleWritable> {
    	 
        public int getPartition(Text key, DoubleWritable value, int numReduceTasks) {
 
        	int ior = numReduceTasks;
        	char fl=key.toString().charAt(0);
        	if (fl=='A'||fl=='B'||fl=='C'||fl=='D'||fl=='E'){return 0;}
        	if (fl=='F'||fl=='G'||fl=='H'||fl=='I'||fl=='J'){return 1;}
        	if (fl=='K'||fl=='L'||fl=='M'||fl=='N'||fl=='O'){return 2;}
        	if (fl=='P'||fl=='Q'||fl=='R'||fl=='S'||fl=='T'){return 3;}

        	else return 4;
    }
    }
    
    
    
    
    
    
    
    public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	    

		double max = 0;
	    while (values.hasNext()) {
		
	    	double newval = values.next().get();
	    	if (newval>max) max=newval;
	    	
	    	
	    }
	    output.collect(key, new DoubleWritable(max));
	}
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), songsMD2.class);
	conf.setJobName("songsMD2");

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(DoubleWritable.class);

	conf.setMapperClass(Map.class);
	conf.setPartitionerClass(Partition.class);
	conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);
	conf.setNumReduceTasks(5);
	
	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	return 0;
    }

    
    
    
    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new songsMD2(), args);
	System.exit(res);
    }
}