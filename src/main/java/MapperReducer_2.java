import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class MapperReducer_2 {
    public static IntWritable CORPUS_0 = new IntWritable(0);
    public static IntWritable CORPUS_1 = new IntWritable(1);
    //<Text, <AmountIncurpus1, AmountInCurpus2>, <AmountOfTimes, CurpusNumber>, AmountOfTimesOppositeCurpus>
    public static class MapperClass extends Mapper<Text, TupleWritable, TupleWritable, IntWritable>{
        @Override
        public void map(Text triplet, TupleWritable amountInEachCurpus, Context context) throws IOException, InterruptedException {
                IntWritable amountOfTimesInCorpus0  =  (IntWritable)amountInEachCurpus.get(0);
                IntWritable amountOfTimesInCorpus1  =  (IntWritable)amountInEachCurpus.get(1);

                TupleWritable corpus0Pair = new TupleWritable(new Writable[]{amountOfTimesInCorpus0,CORPUS_0 });
                TupleWritable corpus1Pair = new TupleWritable(new Writable[]{amountOfTimesInCorpus1,CORPUS_1 });

                //     KEY                              VALUE
                // [ <AmountOfTimes, CorpusNumber>, AmountOfTimesOppositeCorpus ]
                context.write( corpus0Pair,amountOfTimesInCorpus1 );
                context.write( corpus1Pair,amountOfTimesInCorpus0 );
        }
        }
    }

    public static class ReducerClass extends Reducer<TupleWritable, IntWritable, Text, IntWritable>{

        public void reduce(TupleWritable pairCorpusAmount, IntWritable amountInOppositeCorpus, Context context) throws IOException, InterruptedException {






            }

    }




    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word triplets count");
        job.setJarByClass(MapperReducer_2.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(Partitioner.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
