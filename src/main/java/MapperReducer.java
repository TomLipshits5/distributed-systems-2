import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

public class MapperReducer {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, MapWritable>{
        private static IntWritable countWritable = new IntWritable(0);
        private static Text wordPair = new Text();
        private static Text newWord = new Text();

        private static MapWritable wordPairsToStripe = new MapWritable();


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer iterator = new StringTokenizer(value.toString());
            String word1 = "";
            String word2 = "";
            String word3 = "";
            if(iterator.hasMoreTokens()){
                word1 = iterator.nextToken();
            }
            if (iterator.hasMoreTokens()){
                word2 = iterator.nextToken();
                word3 = word2;
                word2 = word1;
            }
            //in each iteration we will assign word1 = word2, word2 = word3, word3 = iterator.nextToken()
            while(iterator.hasMoreTokens()){
                word1 = word2;
                word2 = word3;
                word3 = iterator.nextToken();
                wordPair.set(word1 + " " + word2);
                newWord.set(word3);
                if(wordPairsToStripe.get(wordPair) == null){
                    wordPairsToStripe.put(wordPair, new MapWritable());
                }
                MapWritable stripe = (MapWritable) wordPairsToStripe.get(wordPair);
                if(stripe == null){
                    stripe = new MapWritable();
                }
                countWritable = (IntWritable)stripe.get(newWord);
                if (countWritable == null){
                    countWritable = new IntWritable(0);
                }
                int count = countWritable.get();
                countWritable.set(count + 1);
                stripe.put(newWord, countWritable);
            }

            //Iterate through pairs and write to context the wordPair and the associated map with the pair
            for(Writable keyPair: wordPairsToStripe.keySet()){
                wordPair = (Text) keyPair;
                MapWritable stripe = (MapWritable)wordPairsToStripe.get(wordPair);
                context.write(wordPair, stripe);
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, MapWritable, Text, IntWritable>{

        public void reduce(Text pair, Iterable<MapWritable> maps, Context context) throws IOException, InterruptedException {
            MapWritable sumMap = new MapWritable();
            Iterator<MapWritable> iterator = maps.iterator();
            while(iterator.hasNext()){
                MapWritable currentMap = (MapWritable) iterator.next();
                for(Object key : currentMap.keySet()){
                   Text word3 = (Text) key;
                   if(sumMap.get(word3) == null){
                       sumMap.put(word3, currentMap.get(word3));
                   }else{
                       IntWritable newCountWritable =  (IntWritable)currentMap.get(word3);
                       int newCount = newCountWritable.get();
                       IntWritable oldCountWritable = (IntWritable)sumMap.get(word3);
                       int oldCount = oldCountWritable.get();
                       newCountWritable.set(newCount + oldCount);
                       sumMap.put(word3, newCountWritable);
                   }
                }
            }
            Text tripleWord = new Text();
            for (Object key: sumMap.keySet()){
                String word3 = key.toString();
                String pairString = pair.toString();
                tripleWord.set(pairString + " " + word3);
                context.write(tripleWord, (IntWritable) sumMap.get((Text)key));
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word triplets count");
        job.setJarByClass(MapperReducer.class);
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
