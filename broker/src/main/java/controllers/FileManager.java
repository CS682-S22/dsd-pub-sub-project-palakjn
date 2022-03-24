package controllers;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/**
 * Responsible for handling input/output requests to the file.
 *
 * @author Palak Jain
 */
public class FileManager {

    /**
     * Creates the directory to the given location
     */
    public boolean createDirectory(String parentLocation, String folderLocation) {
        Path path = Path.of(parentLocation, folderLocation);

        if (Files.exists(path) && !deleteDirectory(path)) {
            return false;
        }

        boolean isSuccess = false;

        try {
            Files.createDirectories(path);
            isSuccess = true;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return isSuccess;
    }

    /**
     * Write the data to the given location
     */
    public boolean write(byte[] data, String location) {
        boolean isSuccess = false;

        try (FileOutputStream outputStream = new FileOutputStream(location, true)) {
            outputStream.write(data);
            isSuccess = true;
        } catch (IOException ioException) {
            System.err.printf("Unable to write data to the file %s. Error: %s.\n", location, ioException.getMessage());
        }

        return isSuccess;
    }

    /**
     * Deletes the directory in the local
     */
    private boolean deleteDirectory(Path path) {
        boolean isSuccess = false;

        try {
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
            isSuccess = true;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return isSuccess;
    }
}
