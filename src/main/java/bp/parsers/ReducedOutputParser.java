package bp.parsers;

import bp.utils.Utility;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ReducedOutputParser {
    private static final Logger LOG = Logger.getLogger(ReducedOutputParser.class.getName());

    private final Pattern queryObjectUriPattern = Pattern.compile("IDquery;(.*)");
    private final Pattern dataObjectUriPattern = Pattern.compile("(^(?!IDquery;).*)");
    private final BufferedReader reader;
    private Matcher queryObjectUriMatcher;
    private Matcher dataObjectUriMatcher;
    private String currentQueryUri = "";
    private Set<String> currentObjectUris = new HashSet<>();
    private String nextQueryUri = "";

    public ReducedOutputParser(String filePath) throws IOException {
        reader = new BufferedReader(new InputStreamReader(Utility.openInputStream(filePath)));
        String line = reader.readLine();
        queryObjectUriMatcher = queryObjectUriPattern.matcher(line);
        nextQueryUri = queryObjectUriMatcher.group(1);
    }

    public String getCurrentQueryURI() {
        return currentQueryUri;
    }

    public Set<String> getCurrentObjectUris() {
        return currentObjectUris;
    }

    public void parseNextQueryEvalErrOutput() throws IOException {
        if (nextQueryUri == null)
            return;
        currentQueryUri = nextQueryUri;
        currentObjectUris = new HashSet<>();
        String line = reader.readLine();
        while (line != null) {
            queryObjectUriMatcher = queryObjectUriPattern.matcher(line);
            if (queryObjectUriMatcher.matches()) {
                nextQueryUri = queryObjectUriMatcher.group(1);
                return;
            }
            dataObjectUriMatcher = dataObjectUriPattern.matcher(line);
            if (dataObjectUriMatcher.matches())
                currentObjectUris.add(dataObjectUriMatcher.group(1));
            line = reader.readLine();
        }
        nextQueryUri = null;
    }
}
