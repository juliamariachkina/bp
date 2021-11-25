package bp.parsers;

import bp.utils.Utility;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class SynergyIterator extends ErrOutputIterator {
    private static final Logger LOG = Logger.getLogger(SynergyIterator.class.getName());

    public SynergyIterator(String filePath) throws IOException {
        super(Pattern.compile("IDquery;(.*)"), Pattern.compile("^([^;]*)$"),
                new BufferedReader(new InputStreamReader(Utility.openInputStream(filePath))));
    }

}
