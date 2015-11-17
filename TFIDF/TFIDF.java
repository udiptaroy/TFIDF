package org.myorg;
import java.io.IOException;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TFIDF extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( TFIDF.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new TFIDF(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), "termfrequency");
      job.setJarByClass( this .getClass());
      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      job.setMapperClass( TFMap .class);
      job.setReducerClass( TFReduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( IntWritable .class);
	job.waitForCompletion(true);
	Configuration conf1 = getConf();
        FileSystem fs = FileSystem.get(conf1);
	Path input = new Path(args[0]);
	FileStatus[] inputListStatus = fs.listStatus(input);
        final int countInputFiles = inputListStatus.length;
	Configuration conf = getConf();
        conf.setInt("countInputFiles", countInputFiles);
 	Job job1  = Job .getInstance(conf, "tf-idf");
      job1.setJarByClass( this .getClass());
      FileInputFormat.addInputPaths(job1,  args[1]);
      FileOutputFormat.setOutputPath(job1, new Path(args[ 2]) );
      job1.setMapperClass( IFMap .class);
      job1.setReducerClass( IFReduce .class);
      job1.setOutputKeyClass( Text .class);
      job1.setOutputValueClass( Text .class);
      return job1.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class TFMap extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();
         Text currentWord  = new Text();
	String fileName = ((FileSplit) context.getInputSplit()).getPath().toString();
	fileName=fileName.substring(fileName.lastIndexOf('/') + 1);	
         for ( String word  : WORD_BOUNDARY .split(line)) {
            if (word.isEmpty()) {
               continue;
            }
            currentWord  = new Text(fileName+"#####"+word);
	    context.write(currentWord,one);
         }
      }
   }

   public static class TFReduce extends Reducer<Text ,  IntWritable ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
         throws IOException,  InterruptedException {
         int sum  = 0;
	 double wftd=0;
         for ( IntWritable count  : counts) {
            sum  += count.get();
         }
	if(sum==0)
	wftd=0;
	else
	wftd=1+(Math.log(sum)/Math.log(10));
         context.write(word,  new DoubleWritable(wftd));
      }
   }
      	public static class IFMap extends Mapper<LongWritable ,  Text ,  Text ,  Text > {

	private Text termandFile = new Text();
        private Text termandCounts = new Text();

        public void map( LongWritable key,  Text value,  Context context)
        throws  IOException,  InterruptedException {
	/*Separate the term frequecies from words and file name combination and 
	store word as key and <filename>=<termfrequency> as value*/
        String[] termFreqKeyValue = value.toString().split("\t");
	String[] wordandFile = termFreqKeyValue[0].split("#####");
            termandFile=new Text(wordandFile[1]);
            termandCounts=new Text(wordandFile[0] + "=" + termFreqKeyValue[1]);
            context.write(termandFile, termandCounts);

      }
   }

   public static class IFReduce extends Reducer<Text ,  Text ,  Text ,  Text > {

	private Text word = new Text();

        private Text tfidfCounts = new Text();

      @Override 
      public void reduce( Text key,  Iterable<Text> values,  Context context)
         throws IOException,  InterruptedException {
          // get the number of documents through parameter passed into MapReduce job
            int totalnoofInputFiles = context.getConfiguration().getInt("countInputFiles", 0);
            // number of documents containing the word
            int noofDocswithKey = 0;
            Map<String, String> fileCounts = new HashMap<String, String>();
            for (Text val : values) {
                String[] docAndCounts = val.toString().split("=");
                // increase counter if term frequency is greater than 0
                if (Double.parseDouble(docAndCounts[1]) > 0.0) {
                    noofDocswithKey++;
                }
                fileCounts.put(docAndCounts[0], docAndCounts[1]);
            }
            for (String document : fileCounts.keySet()) {
                // Term frequency is the number occurrences of the term in document
                double tf = Double.valueOf(Double.valueOf(fileCounts.get(document).toString()));

                // Inverse Document Frequency is the number of docs divided by number of docs the 
                // term appears
                double idf = (Math.log((double) totalnoofInputFiles /(double)(noofDocswithKey)))/Math.log(10);

                double tfIdf = tf * idf;
		
                word =new Text(key + "#####" + document);
                tfidfCounts=new Text(tfIdf+"");

                context.write(word,tfidfCounts);
            }
        }

      }
   }
