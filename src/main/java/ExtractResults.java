import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.utils.IoUtils;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;

public class ExtractResults {
    private static S3Client s3 = S3Client.create();
    private static final String BucketName = "ds-2-files-amit";
    private static final String FilePathFormat = "output_3/";

    private static HashMap<String, Float> maxMap = new HashMap<>();
    private static HashMap<String, Float> minMap = new HashMap<>();
    private static String[] words = {"president", "love", "hate", "best", "worst", "god", "gaming", "climate", "Israel", "Tom"};


    private static final Float mapSize = 5F;

    private static String getFileContent(String fileName) throws IOException {
        StringBuilder key = new StringBuilder(FilePathFormat).append(fileName);
        GetObjectRequest request = GetObjectRequest.builder().bucket(BucketName).key(key.toString()).build();
        ResponseInputStream<GetObjectResponse> response = s3.getObject(request);
        return IoUtils.toUtf8String(response);
    }

    private static void  updateMaxMap(String trigram, Float p){
        if(maxMap.size() < mapSize){
            maxMap.put(trigram, p);
        }else{
            Float smallestVal = -1F;
            String smallestKey = findSmallestKey();
            smallestVal = maxMap.get(smallestKey);
            if (p > smallestVal && !maxMap.containsValue(p)){
                maxMap.remove(smallestKey);
                maxMap.put(trigram, p);
            }
        }
    }

    private static String findSmallestKey() {
        Float minVal = (float) -1;
        String minKey = "";
        for (String key : maxMap.keySet()){
            Float val = maxMap.get(key);
            if (minVal == -1 || val < minVal){
                minVal = val;
                minKey = key;
            }
        }
        return minKey;
    }

    private static void  updateMinMap(String trigram, Float p){
        if(minMap.size() < mapSize){
            minMap.put(trigram, p);
        }else{
            Float biggestVal = 2F;
            String biggestKey = findSBiggestKey();
            biggestVal = minMap.get(biggestKey);
            if (p < biggestVal && !minMap.containsValue(p)){
                minMap.remove(biggestKey);
                minMap.put(trigram, p);
            }
        }
    }

    private static String findSBiggestKey() {
        Float maxVal = (float) -1.0;
        String maxKey = "";
        for (String key : minMap.keySet()){
            Float val = minMap.get(key);
            if (maxVal == -1 || val > maxVal){
                maxVal = val;
                maxKey = key;
            }
        }
        return maxKey;
    }

    private static void getResults(String word) throws IOException {
        System.out.println(word);
        String fileNameFormat = "part-r-000";
        for (int i = 0 ; i < 13 ; i++){
            String fileName = "";
            if (i < 10){
                fileName = fileNameFormat + "0" + i;
            }else{
                fileName = fileNameFormat + i;
            }
            System.out.println(fileName);
            String content = getFileContent(fileName);
            BufferedReader reader = new BufferedReader(new StringReader(content));
            String line = reader.readLine();
            while(line != null){
                String trigram = line.split("\t")[0];
                Float p = Float.parseFloat(line.split("\t")[1]);
                if (Arrays.asList(trigram.split(" ")).contains(word)){
                    updateMaxMap(trigram, p);
                    updateMinMap(trigram, p);
                }
                line = reader.readLine();
            }
        }
    }

    private static void writeResForWord(String word) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(word + ".txt"));
        writer.write("MaxMap");
        writer.newLine();
        for (String key : maxMap.keySet()) {
            writer.write(key + "----" + maxMap.get(key));
            writer.newLine();
        }
        writer.write("MinMap");
        writer.newLine();
        for (String key : minMap.keySet()) {
            writer.write(key + "----" + minMap.get(key));
            writer.newLine();
        }
        writer.close();
        System.out.println(word + " HashMap written to file successfully.");
    }

    public static void main(String[] args) throws IOException {
        for (String word : words){
            getResults(word);
            writeResForWord(word);
            maxMap.clear();
            minMap.clear();
        }
    }
}
