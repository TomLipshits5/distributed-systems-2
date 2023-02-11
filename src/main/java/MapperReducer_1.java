import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.utils.IoUtils;
import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Arrays;

public class MapperReducer_1 {
    public static class Mapper_1 extends Mapper<LongWritable, Text, Text, IntWritable>{
        private boolean corpusId = false;
        private final List<String> stopWords = new ArrayList<>(Arrays.asList(new String[]{"a", "about", "above", "across", "after",
                "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also", "although",
                "always", "am", "among", "amongst", "amoungst", "amount", "an", "and", "another", "any", "anyhow",
                "anyone", "anything", "anyway", "anywhere", "are", "around", "as", "at", "back", "be", "became",
                "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being",
                "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom", "but", "by", "call",
                "can", "cannot", "cant", "co", "computer", "con", "could", "couldnt", "cry", "de", "describe",
                "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven", "else",
                "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere",
                "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly",
                "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt",
                "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him",
                "himself", "his", "how", "however", "hundred", "i", "ie", "if", "in", "inc", "indeed", "interest", "into",
                "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many",
                "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much",
                "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody",
                "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only",
                "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own", "part", "per",
                "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious",
                "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow",
                "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten",
                "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby",
                "therefore", "therein", "thereupon", "these", "they", "thick", "thin", "third", "this", "those", "though",
                "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve",
                "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what",
                "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon",
                "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will",
                "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves", "cream"}));
        public List<String> initStopWords(){
            List<String> stopWords = new ArrayList<>();
            try{
                S3Client s3Client = S3Client.create();
                GetObjectRequest request = GetObjectRequest.builder().bucket(Deployment.BucketName).key(Deployment.StopWordsPath).build();
                ResponseInputStream<GetObjectResponse> object = s3Client.getObject(request);
                String content = IoUtils.toUtf8String(object);
                BufferedReader bufferReader = new BufferedReader(new StringReader(content));
                String line;
                while((line = bufferReader.readLine()) != null){
                    stopWords.add(line.trim());
                }
                bufferReader.close();
                return stopWords;
            }catch(Exception e){
                System.err.println("cant open stop words file, return with error: " + e);
                return null;
            }
        }
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String trigramString = Arrays.asList(value.toString().split("\t")).get(0);
            List<String> trigramList = Arrays.asList(trigramString.split(" "));
            if (validWords(trigramList, stopWords)){
                Text trigram = new Text(String.join(" ",trigramList.get(0), trigramList.get(1), trigramList.get(2)));
                IntWritable corpus = corpusId ? new IntWritable(1) : new IntWritable(0);
                System.out.println(trigram);
                context.write(trigram, corpus);
                corpusId = !corpusId;
            }
        }

        private String printStopWords(List<String> stopWords) {
            StringBuilder out = new StringBuilder();
            for(String word : stopWords){
                out.append("\"").append(word).append("\",");
            }
            return out.toString();
        }

        public boolean validWords(List<String> trigram, List<String> stopWords){
            if (trigram.size() != 3){
                return false;
            }
            for (String word: trigram){
                if(!validWord(word) || stopWords.contains(word)){
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
    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step_1");
        job.setJarByClass(MapperReducer_1.class);
        job.setMapperClass(MapperReducer_1.Mapper_1.class);
        job.setPartitionerClass(PartitionerClass.class);
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
