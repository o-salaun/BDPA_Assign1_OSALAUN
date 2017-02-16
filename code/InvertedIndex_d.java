package stopwordcount; // put name of the package

import java.io.IOException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndex_d extends Configured implements Tool {
  
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new InvertedIndex_d(), args);
      
      System.exit(res);
   }

   // DRIVER CLASS

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "InvertedIndex_d");

      job.setJarByClass(InvertedIndex_d.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      
      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ; "); // added for making output file
      job.setNumReduceTasks(10); // use 10 reducers

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      FileSystem fs = FileSystem.newInstance(getConf());

      if (fs.exists(new Path(args[1]))) { // deletes and replaces existing folder or output files
         fs.delete(new Path(args[1]), true);
      }

      job.waitForCompletion(true);
      
      return 0;
   }
   
   // MAP CLASS
   
   public static class Map extends Mapper<LongWritable, Text, Text, Text> {
      
      private Text word = new Text(); // define key of mapper output
      private Text filename = new Text(); // define value of mapper output

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {

         HashSet<String> stopwords = new HashSet<String>();
         BufferedReader Reader = new BufferedReader(
               new FileReader(new File("/home/cloudera/workspace/InvertedIndex_d/StopWords.csv"))); // input stopwords file
         String pattern;
         while ((pattern = Reader.readLine()) != null) {
            stopwords.add(pattern.toLowerCase()); // store stop words set in lower case into a hashset
         }

         String filenameStr = ((FileSplit) context.getInputSplit()).getPath().getName();
         filename = new Text(filenameStr);


         for (String string: value.toString().split("\\s+")) {
            if (!stopwords.contains(string.toLowerCase())) {
               word.set(string.toLowerCase()); // set all non stop words to lower case
            }
         }

         context.write(word, filename); // generate key-value pairs of mapper output
         
      }
   }

   // COMBINER
   
   public static class Combine extends Reducer<Text, Text, Text, Text> {

      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {

         StringBuilder builder = new StringBuilder();

         context.write(key, new Text(builder.toString()));

      }
   }
 


   
   // REDUCE FUNCTION
   
   public static class Reduce extends Reducer<Text, Text, Text, Text> {

      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {

         HashMap<String, Integer> container = new HashMap<String, Integer>(); 
         // define a container for pairs of string + integer
         // it will correspond to <file name, number of occurences of the key-word in the file>
         
         for (Text value : values) {
         if (!container.containsKey(value.toString())) {
        	 container.put(value.toString(),1);
        	// if the value/file name is stored for the 1st time, set value at 1 
         }else{
        	
        	 int count = container.get(value.toString()) + 1;
        	 container.put(value.toString(), count);
          // otherwise, if the value is already stored, increase the value by 1
              }
         }
         
         StringBuilder builder = new StringBuilder();
         // in the value of the reducer output, the filenames are separated by commas and spaces
         String separator = "";
         for (String value : container.keySet()) {
            builder.append(separator);
            separator = "; ";
            builder.append(value + "#" + container.get(value));
         }

         context.write(key, new Text(builder.toString())); // generate key-value pairs of reducer output

      }
   }
}
