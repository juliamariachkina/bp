package bp.utils;

import messif.objects.LocalAbstractObject;
import messif.objects.util.AbstractObjectList;
import messif.objects.util.StreamGenericAbstractObjectIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Utility {

    private static final Logger LOG = Logger.getLogger(Utility.class.getName());

    public static Iterator<LocalAbstractObject> getObjectsIterator(String filePath,
                                                                   Class<LocalAbstractObject> className) throws IOException {
            return new StreamGenericAbstractObjectIterator<LocalAbstractObject>(className, filePath);
    }

    public static List<LocalAbstractObject> getObjectsList(String filePath,
                                                           Class<LocalAbstractObject> className,
                                                           int objCount) throws IOException {
            return new AbstractObjectList<>(getObjectsIterator(filePath, className), objCount);
    }


}
