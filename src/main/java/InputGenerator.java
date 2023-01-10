
    import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class InputGenerator {
    public static void main(String[] args) {
        String[] words = {"apple", "banana", "cherry", "date", "elderberry", "fig", "grape", "huckleberry", "ice" ,"cream"};
        Random random = new Random();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter("src/main/java/input-file.txt"))) {
            int i = 0;
            while (i < 5) {
                // Generate a random row
                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < 3; j++) {
                    sb.append(words[random.nextInt(words.length)]).append("\t");
                }
                sb.append(random.nextInt(10) + 1).append("\t");
                sb.append(random.nextInt(10) + 1).append("\n");

                // Write the row to the file
                writer.write(sb.toString());
                i++;
                // Write an identical row with a 30% probability
                while(random.nextInt(10) < 5 && i < 5) {
                    writer.write(sb.toString());
                    i++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


