package task1;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class GenerateTextFile {
    private static final int FILE_COUNT = 100;
    private static final int LINES_COUNT = 100000;
    private static final String OUTPUT_DIR = "src/task1/output/";
    private static final Random random = new Random();
    private static final String LATIN_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    private static final String RUSSIAN_CHARS = "АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯабвгдеёжзийклмнопрстуфхцчшщъыьэюя";

    public static void main(String[] args) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime()
                .availableProcessors());

        for (int i = 0; i < FILE_COUNT; i++) {
            String filename = OUTPUT_DIR + "outputFile_" + (i+1) + ".txt";
            executor.submit(() -> {
                try {
                    generateFile(filename, LINES_COUNT);
                    System.out.println(filename + " create!");
                } catch (IOException e) {
                    System.err.println("Error file: " + e.getMessage());
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);
        System.out.println("All files created!");
    }

    public static void generateFile(String filename, int linesCount) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < linesCount; i++) {
                sb.setLength(0);
                sb.append(generateRandomDate()).append("||")
                        .append(generateRandomLatinString(10)).append("||")
                        .append(generateRandomRussianString(10)).append("||")
                        .append(generateRandomEvenInteger()).append("||")
                        .append(generateRandomDecimal()).append("||");
                writer.write(sb.toString());
                writer.newLine();
                if ((i & 0x3FF) == 0) writer.flush();
            }
        }
    }

    private static String generateRandomDate() {
        LocalDate today = LocalDate.now();
        LocalDate fiveYearsAgo = today.minusYears(5);

        long startEpochDay = fiveYearsAgo.toEpochDay();
        long endEpochDay = today.toEpochDay();
        long randomDays = startEpochDay + random.nextLong(endEpochDay - startEpochDay + 1);

        return LocalDate.ofEpochDay(randomDays)
                .format(DateTimeFormatter.ofPattern("dd.MM.yyyy"));
    }

    private static String generateRandomLatinString(int length) {
        return generateRandomString(LATIN_CHARS, length);
    }

    private static String generateRandomRussianString(int length) {
        return generateRandomString(RUSSIAN_CHARS, length);
    }

    private static String generateRandomString(String characterSet, int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int index = random.nextInt(characterSet.length());
            sb.append(characterSet.charAt(index));
        }
        return sb.toString();
    }

    private static int generateRandomEvenInteger() {
        return (random.nextInt(50_000_000) + 1) * 2;
    }

    private static String generateRandomDecimal() {
        double number = 1.0 + random.nextDouble() * 19.0;

        DecimalFormat df = new DecimalFormat("0.00000000");
        DecimalFormatSymbols symbols = new DecimalFormatSymbols(Locale.US);
        df.setDecimalFormatSymbols(symbols);

        return df.format(number);
    }

}
