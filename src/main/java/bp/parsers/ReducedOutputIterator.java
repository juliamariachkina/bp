package bp.parsers;

import bp.utils.Utility;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class ReducedOutputIterator extends ErrOutputIterator {
    private static final Logger LOG = Logger.getLogger(ReducedOutputIterator.class.getName());

    public ReducedOutputIterator(String filePath) throws IOException {
        super(Pattern.compile("IDquery;(.*)"), Pattern.compile("(^(?!IDquery;).*)"),
                new BufferedReader(new InputStreamReader(Utility.openInputStream(filePath))));
    }

}
