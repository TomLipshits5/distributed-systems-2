import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import java.util.Arrays;

public class MapperReducer_1 {
    public static class Mapper_1 extends Mapper<LongWritable, Text, Text, IntWritable>{
        private boolean corpusId = false;
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            List<String> trigramList = Arrays.asList(value.toString().split("\t")).subList(0,3);
            if (validWords(trigramList)){
                Text trigram = new Text(String.join(" ",trigramList.get(0), trigramList.get(1), trigramList.get(2)));
                IntWritable corpus = corpusId ? new IntWritable(1) : new IntWritable(0);
                System.out.println(trigram);
                context.write(trigram, corpus);
                corpusId = !corpusId;
            }
        }

        public boolean validWords(List<String> trigram){
            if (trigram.size() != 3){
                return false;
            }
            for (String word: trigram){
                if(!validWord(word)){
                    return false;
                }
            }
            return true;
        }

        private boolean validWord(String word) {
            return word.length() > 0 && allLetters(word);
        }

        private boolean allLetters(String word) {
            for (int i = 0 ; i < word.length() ; i++){
                if (!Character.isLetter(word.charAt(i))){
                    return false;
                }
            }
            return true;
        }
    }


    public static class Reducer_1 extends Reducer<Text, IntWritable, Text, TupleWritable>{
        @Override
        public void reduce(Text key, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
            Iterator<IntWritable> it = counts.iterator();
            IntWritable r0 = new IntWritable(0);
            IntWritable r1 = new IntWritable(0);
            while(it.hasNext()){
                IntWritable corpusId = it.next();
                if (corpusId.get() == 0){
                    r0.set(r0.get() + 1);
                }else{
                    r1.set(r1.get() + 1);
                }
            }
            TupleWritable result = new TupleWritable(r0.get(), r1.get());
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step_1");
        job.setJarByClass(MapperReducer_1.class);
        job.setMapperClass(MapperReducer_1.Mapper_1.class);
        job.setPartitionerClass(Partitioner.class);
        job.setReducerClass(MapperReducer_1.Reducer_1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TupleWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
