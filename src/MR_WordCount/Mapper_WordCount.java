package MR_WordCount;

import java.io.IOException;    
import java.util.StringTokenizer;    
import org.apache.hadoop.io.IntWritable;    
import org.apache.hadoop.io.LongWritable;    
import org.apache.hadoop.io.Text;    
import org.apache.hadoop.mapred.MapReduceBase;    
import org.apache.hadoop.mapred.Mapper;    
import org.apache.hadoop.mapred.OutputCollector;    
import org.apache.hadoop.mapred.Reporter;  
/**
 * This is Mapper class for word count. I referenced some codes from this website https://www.javatpoint.com/mapreduce-word-count-example and 
 * made some modifications. For instance, when this program spilts the text into different words, I replace all symbols, like comma£¬ period and dash, 
 * with " ". Therefore, the result is more reasonable. 
 *
 */
public class Mapper_WordCount extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{    
    private final static IntWritable one = new IntWritable(1);    
    private Text word = new Text();    
    public void map(LongWritable key, Text value,OutputCollector<Text,IntWritable> output,     
           Reporter reporter) throws IOException{    
        String line = value.toString();
        String lineNoSymbols = line.replaceAll("\\p{P}" , "");
        StringTokenizer  tokenizer = new StringTokenizer(lineNoSymbols);    
        while (tokenizer.hasMoreTokens()){    
            word.set(tokenizer.nextToken());    
            output.collect(word, one);    
        }    
    }    
    
}  