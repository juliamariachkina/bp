package bp.utils;

import messif.objects.LocalAbstractObject;
import messif.objects.util.AbstractObjectList;
import messif.objects.util.StreamGenericAbstractObjectIterator;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility class encapsulates helper methods for this project.
 */
public class Utility {

    private static final Logger LOG = Logger.getLogger(Utility.class.getName());

    public static <T extends LocalAbstractObject, S extends T, I extends Iterator<T>> I getObjectsIterator(String filePath, Class<S> className) {
        try {
            return (I) new StreamGenericAbstractObjectIterator<T>(className, filePath);
        } catch (Exception e) {
            LOG.log(Level.SEVERE, e.getMessage());
            return null;
        }
    }

    public static <T extends LocalAbstractObject> List<T> getObjectsList(String filePath, Class<T> className, int objCount) {
        return new AbstractObjectList<T>(Objects.requireNonNull(getObjectsIterator(filePath, className)), objCount);
    }
}
