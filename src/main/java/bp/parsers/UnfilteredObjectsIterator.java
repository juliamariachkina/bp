package bp.parsers;

import bp.utils.Utility;

import java.io.*;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class UnfilteredObjectsIterator extends ErrOutputIterator {
    private static final Logger LOG = Logger.getLogger(UnfilteredObjectsIterator.class.getName());

    public UnfilteredObjectsIterator(String filePath) throws IOException {
        super(Pattern.compile("INFO: Query object with uri: (.*)"), Pattern.compile("(.*);(.*)"),
                new BufferedReader(new InputStreamReader(Utility.openInputStream(filePath))));
    }
}
