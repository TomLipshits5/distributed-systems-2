import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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
    public static class Mapper2Class extends Mapper<LongWritable, Text, IntWritable, TupleWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String amountsString = Arrays.asList(value.toString().split("\t")).get(1);

            IntWritable amountOfTimesInCorpus0 = new IntWritable(Integer.parseInt(Arrays.asList(amountsString.split(" ")).get(0)));
            IntWritable amountOfTimesInCorpus1 = new IntWritable(Integer.parseInt(Arrays.asList(amountsString.split(" ")).get(1)));

            TupleWritable corpus0Pair = new TupleWritable(CORPUS_0.get(), amountOfTimesInCorpus1.get());
            TupleWritable corpus1Pair = new TupleWritable(CORPUS_1.get(), amountOfTimesInCorpus0.get());

            System.out.println(amountOfTimesInCorpus0 + " " + corpus0Pair);
            System.out.println(amountOfTimesInCorpus1 + " " + corpus1Pair);

            context.write(amountOfTimesInCorpus0, corpus0Pair);
            context.write(amountOfTimesInCorpus1, corpus1Pair);
        }

        // [ AmountOfTimes, <CorpusNumber AmountOfTimesOppositeCorpus> ]
    }
        public static class Reducer2Class extends Reducer<IntWritable, TupleWritable, IntWritable, TupleWritable> {
            @Override
            public void reduce(IntWritable amountInCorpus, Iterable<TupleWritable> amountAndOpposite, Context context) throws IOException, InterruptedException {

                Iterator<TupleWritable> iterator = amountAndOpposite.iterator();
                IntWritable T0 = new IntWritable(0);
                IntWritable N0 = new IntWritable(0);
                IntWritable T1 = new IntWritable(0);
                IntWritable N1 = new IntWritable(0);
                IntWritable R = amountInCorpus;

                while (iterator.hasNext()) {
                    TupleWritable record = iterator.next();
                    IntWritable CorpusNumber = new IntWritable(record.getInt1());
                    IntWritable AmountInOppositeCorpusNumber = new IntWritable((record.getInt2()));

                    if (CorpusNumber.get() == 0) {
                        N0 = new IntWritable(N0.get() + 1);
                        T1 = new IntWritable(T1.get() + AmountInOppositeCorpusNumber.get());
                    } else {
                        N1 = new IntWritable(N1.get() + 1);
                        T0 = new IntWritable(T0.get() + AmountInOppositeCorpusNumber.get());
                    }
                }

                IntWritable N0_N1 = new IntWritable(N1.get() + N0.get());
                IntWritable T0_T1 = new IntWritable(T1.get() + T0.get());
                context.write(R, new TupleWritable(N0_N1.get(), T0_T1.get()));

            }

        }

        // [ R, <N0+N1, T0+T1>]


        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "ssss");
            job.setJarByClass(MapperReducer_2.class);
            job.setMapperClass(MapperReducer_2.Mapper2Class.class);
            job.setPartitionerClass(Partitioner.class);
            job.setReducerClass(MapperReducer_2.Reducer2Class.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(TupleWritable.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(TupleWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }

