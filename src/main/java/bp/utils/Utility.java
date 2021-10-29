package bp.utils;

import messif.objects.LocalAbstractObject;
import messif.objects.util.AbstractObjectList;
import messif.objects.util.StreamGenericAbstractObjectIterator;

import java.io.*;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Utility class encapsulates helper methods for this project.
 */
public class Utility {

    private static final Logger LOG = Logger.getLogger(Utility.class.getName());

    public static <T extends LocalAbstractObject, S extends T> StreamGenericAbstractObjectIterator<T> getObjectsIterator(String filePath, Class<S> className) {
        try {
            return new StreamGenericAbstractObjectIterator<>(className, filePath);
        } catch (Exception e) {
            LOG.log(Level.SEVERE, e.getMessage());
            return null;
        }
    }

    public static <T extends LocalAbstractObject> List<T> getObjectsList(String filePath, Class<T> className, int objCount) {
        return new AbstractObjectList<T>(Objects.requireNonNull(getObjectsIterator(filePath, className)), objCount);
    }

    public static PrintStream getPrintStream(File file) {
        return new PrintStream(Utility.getOutputStream(file));
    }

    public static PrintStream getPrintStream(String path) {
        return new PrintStream(Utility.getOutputStream(path));
    }

    public static OutputStream getOutputStream(File file) {
        return Utility.getOutputStream(file.getAbsolutePath());
    }

    public static OutputStream getOutputStream(String path) {
        try {
            if (path.toLowerCase().endsWith("gz"))
                return new GZIPOutputStream(new FileOutputStream(path));
            return new FileOutputStream(path);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        return null;
    }

    public static InputStream openInputStream(File file) throws IOException {
        return Utility.openInputStream(file.getAbsolutePath());
    }

    public static InputStream openInputStream(String path) throws IOException {
        if (path.toLowerCase().endsWith("gz"))
            return new GZIPInputStream(new FileInputStream(path));
        else
            return new FileInputStream(path);
    }
}
