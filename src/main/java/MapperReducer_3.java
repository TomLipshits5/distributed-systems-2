import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class MapperReducer_3 {
    private static final BigDecimal N = new BigDecimal("23260642968");
    private static boolean IsNumeric(String s){
        for(Character c : s.toCharArray()){
            if(!Character.isDigit(c))
                return false;
        }
        return true;
    }
    public static class Mapper_3 extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            List<String> valueList = Arrays.asList(value.toString().split("\t"));
            if (IsNumeric(valueList.get(0))){
                Integer r = Integer.parseInt(valueList.get(0));
                context.write(new Text(r + " " + "0"), new Text(valueList.get(1)));
            }else{
                Text trigram = new Text(valueList.get(0));
                int r0 = Integer.parseInt(valueList.get(1).split(" ")[0]);
                context.write(new Text( r0 + " " + "1"), trigram);
            }
        }
    }

    public static class Comperator_3 extends WritableComparator {
        protected Comperator_3(){
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable t1, WritableComparable t2){
            Text word1 = (Text) t1;
            Text word2 = (Text) t2;
            String[] l1 = word1.toString().split(" ");
            String[] l2 = word2.toString().split(" ");
            if (IsNumeric(l1[0]) && IsNumeric(l2[0])){
                int r1 = Integer.parseInt(l1[0]);
                int r2 = Integer.parseInt(l2[0]);
                return Integer.compare(r1, r2);
            }
            System.out.println("error the key is not numeric");
            return 0;
        }
    }

    public static class Reducer_3 extends Reducer<Text, Text, Text, FloatWritable>{
        @Override
        public void reduce(Text r, Iterable<Text> trigrams, Context context) throws IOException, InterruptedException {
            FloatWritable res = new FloatWritable();
            Iterator<Text> it = trigrams.iterator();
            if (it.hasNext()){
                String[] numValues = it.next().toString().split(" ");
                BigDecimal T =  new BigDecimal(((Integer)Integer.parseInt(numValues[0])).toString());
                BigDecimal Ni = new BigDecimal(((Integer)Integer.parseInt(numValues[1])).toString());
                BigDecimal N_Ni = N.multiply(new BigDecimal(Ni.toString()));
                res.set(T.divide(N_Ni, MathContext.DECIMAL32).floatValue());
            }
            while(it.hasNext()){
                Text trigram = it.next();
                context.write(trigram, res);
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, FloatWritable> {
        @Override
        public int getPartition(Text key, FloatWritable value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step_3");
        job.setJarByClass(MapperReducer_3.class);
        job.setMapperClass(MapperReducer_3.Mapper_3.class);
        job.setGroupingComparatorClass(Comperator_3.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(MapperReducer_3.Reducer_3.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
