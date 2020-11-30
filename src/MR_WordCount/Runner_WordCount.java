package MR_WordCount;

import java.io.IOException;    
import org.apache.hadoop.fs.Path;    
import org.apache.hadoop.io.IntWritable;    
import org.apache.hadoop.io.Text;    
import org.apache.hadoop.mapred.FileInputFormat;    
import org.apache.hadoop.mapred.FileOutputFormat;    
import org.apache.hadoop.mapred.JobClient;    
import org.apache.hadoop.mapred.JobConf;    
import org.apache.hadoop.mapred.TextInputFormat;    
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * This is Runner class for word count, which including the main method to run the whole MapReduce program. I referenced some codes 
 * from this website https://www.javatpoint.com/mapreduce-word-count-example and made some modifications. 
 * 
 *
 */
public class Runner_WordCount {    
    public static void main(String[] args) throws IOException{    
        JobConf conf = new JobConf(Runner_WordCount.class);    
        conf.setJobName("WordCount");    
        conf.setOutputKeyClass(Text.class);    
        conf.setOutputValueClass(IntWritable.class);            
        conf.setMapperClass(Mapper_WordCount.class);    
        conf.setCombinerClass(Reducer_WordCount.class);    
        conf.setReducerClass(Reducer_WordCount.class);         
        conf.setInputFormat(TextInputFormat.class);    
        conf.setOutputFormat(TextOutputFormat.class);           
        FileInputFormat.setInputPaths(conf,new Path(args[0]));    
        FileOutputFormat.setOutputPath(conf,new Path(args[1]));     
        JobClient.runJob(conf);    
    }    
}    