package task3;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class ImporterFilesDB {
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/test_db";
    private static final String DB_USER = "postgres";
    private static final String DB_PASSWORD = "445566";
    private static final String FOLDER_PATH = "src/task1/output/";

    public static void main(String[] args) {
        try {
            createTableIfNotExists();
            importFilesParallel();
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void createTableIfNotExists() throws SQLException {
        String createTableSQL = """
                CREATE TABLE IF NOT EXISTS imported_data (
                    id SERIAL PRIMARY KEY,
                    data_value DATE,
                    latin_text VARCHAR(50),
                    russian_text VARCHAR(50),
                    even_number BIGINT,
                    decimal_value NUMERIC(12,8),
                    CONSTRAINT unique_row UNIQUE (data_value, latin_text, russian_text, even_number, decimal_value)
                );
                """;

        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             Statement statement = connection.createStatement()){
            statement.execute(createTableSQL);
            System.out.println("Таблица imported_data готова");
        }
    }

    public static void importFilesParallel() throws IOException, InterruptedException {
        Path dir = Paths.get(FOLDER_PATH);
        if (!Files.exists(dir)) {
            throw new FileNotFoundException("Папка не найдена: " + FOLDER_PATH);
        }

        List<Path> files = Files.list(dir)
                .filter(p -> p.toString().endsWith(".txt"))
                .sorted()
                .toList();

        if (files.isEmpty()) {
            System.out.println("Нет файлов");
            return;
        }

        ExecutorService executor = Executors.newFixedThreadPool(Math.min(files.size(), 4));
        List<Future<Void>> futures = new ArrayList<>();
        for (Path file : files) {
            futures.add(executor.submit(() -> {
                importFile(file);
                return null;
            }));
        }

        for (Future<Void> f: futures) {
            try {
                f.get();
            } catch (ExecutionException | InterruptedException e) {
                System.err.println("Ошибка при импорте: " + e.getCause());
            }
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);
        System.out.println("\nВсе файлы импортированы успешно!");
    }

    private static void importFile(Path file) {
        String insertSQL = """
                INSERT INTO imported_data (data_value, latin_text, russian_text, even_number, decimal_value)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (data_value, latin_text, russian_text, even_number, decimal_value) DO NOTHING
                """;

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd.MM.yyyy");

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             PreparedStatement stmt = conn.prepareStatement(insertSQL);
             BufferedReader reader = Files.newBufferedReader(file)) {

            conn.setAutoCommit(false);

            long totalLines = Files.lines(file).count();
            long processed = 0;
            long skipped = 0;
            String line;

            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\s*\\|\\|\\s*");
                if (parts.length < 5) {
                    skipped++;
                    continue;
                }

                try {
                    LocalDate date = LocalDate.parse(parts[0].trim(), formatter);
                    String latin = parts[1].trim();
                    String russian = parts[2].trim();
                    long even = Long.parseLong(parts[3].trim());
                    double dec = Double.parseDouble(parts[4].trim());

                    stmt.setDate(1, Date.valueOf(date));
                    stmt.setString(2, latin);
                    stmt.setString(3, russian);
                    stmt.setLong(4, even);
                    stmt.setBigDecimal(5, new java.math.BigDecimal(dec));

                    stmt.addBatch();
                } catch (Exception e) {
                    skipped++;
                }
                processed++;

                if (processed % 1000 == 0) {
                    stmt.executeBatch();
                    conn.commit();
                    printProgress(processed, totalLines);
                }
            }

            stmt.executeBatch();
            conn.commit();

            System.out.printf("%n %s импорт завершён (%d строк, пропущено %d)%n",
                    file.getFileName(), processed - skipped, skipped);

        } catch (SQLException | IOException e) {
            System.err.println("Ошибка при импорте файла " + file.getFileName() + ": " + e.getMessage());
        }

    }

    private static void printProgress(long processed, long total) {
        double percent = (processed * 100.0 / total);
        System.out.printf("\rПрогресс: %.2f%% (%d / %d)", percent, processed, total);
    }
}