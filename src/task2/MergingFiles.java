package task2;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;

public class MergingFiles {
    private static final String OUTPUT_DIR = "src/task2/";
    private static final String FOLDER_PATH = "src/task1/output/";

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        String mergedFilePath = OUTPUT_DIR + "mergedFile.txt";

        System.out.print("Введите фильтр для удаления строк (например: abc): ");
        String filter = scanner.nextLine().trim();

        try {
            int deleted = mergedFiles(mergedFilePath, filter);
            System.out.println("\nОбъединение завершено!");
            System.out.println("Удалено строк: " + deleted);
            System.out.println("Результат сохранён в: " + mergedFilePath);
        } catch (IOException | InterruptedException e) {
            System.out.println("Ошибка при объединении: " + e.getMessage());
        }
        scanner.close();
    }

    public static int mergedFiles(String mergedFilePath, String filter)
            throws IOException, InterruptedException  {
        Path dir = Paths.get(FOLDER_PATH);
        if (!Files.exists(dir)) {
            throw new FileNotFoundException("Папка не найдена: " + FOLDER_PATH);
        }

        List<Path> files = Files.list(dir)
                .filter(p -> p.toString().endsWith(".txt") && !p.getFileName().toString().equals("merged.txt"))
                .sorted()
                .toList();

        if (files.isEmpty()) {
            System.out.println("Нет файлов для объединения");
            return 0;
        }

        int threads = Math.min(files.size(), Runtime.getRuntime().availableProcessors());
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        List<Future<FilePartResult>> futures = new ArrayList<>();

        System.out.println("Используется потоков: " + threads);
        System.out.println("Найдено файлов: " + files.size());


        for (Path file : files) {
            futures.add(executor.submit(() -> processFile(file, filter, FOLDER_PATH)));
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);

        int totalDeleted = 0;

        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(mergedFilePath),
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {

            for (Future<FilePartResult> future : futures) {
                FilePartResult result = future.get();
                totalDeleted += result.deletedLines;

                Files.lines(result.tempFile).forEach(line -> {
                    try {
                        writer.write(line);
                        writer.newLine();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });

                Files.deleteIfExists(result.tempFile);
            }
        } catch (ExecutionException e) {
            throw new IOException("Ошибка в одном из потоков: " + e.getCause().getMessage());
        }

        return totalDeleted;
    }

    private static FilePartResult processFile(Path inputFile, String filter, String outputDir) {
        int deleted = 0;
        try {
            Path tempFile = Files.createTempFile(Paths.get(outputDir), "part_", ".tmp");

            try (BufferedReader reader = Files.newBufferedReader(inputFile);
                 BufferedWriter writer = Files.newBufferedWriter(tempFile)) {

                String line;
                while ((line = reader.readLine()) != null) {
                    if (!filter.isEmpty() && line.contains(filter)) {
                        deleted++;
                        continue;
                    }
                    writer.write(line);
                    writer.newLine();
                }
            }

            System.out.println("Обработан: " + inputFile.getFileName() + " (удалено строк: " + deleted + ")");
            return new FilePartResult(tempFile, deleted);
        } catch (IOException e) {
            throw new UncheckedIOException("Ошибка обработки " + inputFile.getFileName() + ": " + e.getMessage(), e);
        }
    }
}