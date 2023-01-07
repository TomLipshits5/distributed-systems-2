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



    //[ Text, <Amount in Corpus0, Amount in Corpus1> ]
    public static class Mapper2Class extends Mapper<Text, TupleWritable, IntWritable, TupleWritable>{
        @Override
        public void map(Text triplet, TupleWritable amountInEachCurpus, Context context) throws IOException, InterruptedException {
                IntWritable amountOfTimesInCorpus0  =  (IntWritable)amountInEachCurpus.get(0);
                IntWritable amountOfTimesInCorpus1  =  (IntWritable)amountInEachCurpus.get(1);

                TupleWritable corpus0Pair = new TupleWritable(new Writable[]{CORPUS_0, amountOfTimesInCorpus1 });
                TupleWritable corpus1Pair = new TupleWritable(new Writable[]{CORPUS_1,amountOfTimesInCorpus0 });

                context.write( amountOfTimesInCorpus0 ,corpus0Pair );
                context.write( amountOfTimesInCorpus1,corpus1Pair );
        }
        }
    // [ AmountOfTimes, <CorpusNumber AmountOfTimesOppositeCorpus> ]

    public static class Reducer2Class extends Reducer<TupleWritable, IntWritable, IntWritable, TupleWritable>{

        public void reduce(IntWritable amountInCorpus, Iterable<TupleWritable>  amountAndOpposite, Context context) throws IOException, InterruptedException {

            Iterator<TupleWritable> iterator = amountAndOpposite.iterator();

            IntWritable T0 = new IntWritable(0);
            IntWritable N0 = new IntWritable(0);
            IntWritable T1 = new IntWritable(0);
            IntWritable N1 = new IntWritable(0);
            IntWritable R  = amountInCorpus;

            while(iterator.hasNext()) {
                TupleWritable record = iterator.next();
                IntWritable CorpusNumber = (IntWritable)((record.get(0)));
                IntWritable AmountInOppositeCorpusNumber = (IntWritable)((record.get(1)));

                if(CorpusNumber.get() ==  0 ) {
                    N0 = new IntWritable( N0.get() + 1);
                    T1 = new IntWritable(T1.get() + AmountInOppositeCorpusNumber.get());
                }else {
                    N1 = new IntWritable( N1.get() + 1);
                    T0 = new IntWritable(T0.get() + AmountInOppositeCorpusNumber.get());
                }
            }

            IntWritable N0_N1 = new IntWritable(N1.get()+ N0.get());
            IntWritable T0_T1 =  new IntWritable(T1.get()+ T0.get());
            context.write(R , new TupleWritable(new Writable[]{N0_N1,T0_T1}));

            }

    }

    // [ R, <N0+N1, T0+T1>]



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word triplets count");
        job.setJarByClass(MapperReducer_2.class);
        job.setMapperClass(Mapper2Class.class);
        job.setPartitionerClass(Partitioner.class);
        job.setReducerClass(Reducer2Class.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
