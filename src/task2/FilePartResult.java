package task2;

import java.nio.file.Path;

public class FilePartResult {
    final Path tempFile;
    final int deletedLines;

    FilePartResult(Path tempFile, int deletedLines) {
        this.tempFile = tempFile;
        this.deletedLines = deletedLines;
    }
}
