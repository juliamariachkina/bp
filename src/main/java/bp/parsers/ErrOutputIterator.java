package bp.parsers;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A generic class that parses error outputs produced during query evaluations.
 */
public class ErrOutputIterator {
    private static final Logger LOG = Logger.getLogger(ErrOutputIterator.class.getName());

    private final Pattern queryObjectUriPattern;
    private final Pattern dataObjectUriPattern;
    private final BufferedReader reader;
    private Matcher queryObjectUriMatcher;
    private String currentQueryUri = "";
    private List<String> currentObjectUris = new ArrayList<>();
    private String nextQueryUri = "";

    public ErrOutputIterator(Pattern queryObjectUriPattern, Pattern dataObjectUriPattern, BufferedReader reader) throws IOException {
        this.queryObjectUriPattern = queryObjectUriPattern;
        this.dataObjectUriPattern = dataObjectUriPattern;
        this.reader = reader;

        do {
            String line = reader.readLine();
            queryObjectUriMatcher = queryObjectUriPattern.matcher(line);
        } while (!queryObjectUriMatcher.matches());
        nextQueryUri = queryObjectUriMatcher.group(1);
    }

    /**
     * @return currently processed query URI
     */
    public String getCurrentQueryURI() {
        return currentQueryUri;
    }

    /**
     * @return list of object URIs to which the distances from the currentQueryUri were evaluated
     */
    public List<String> getCurrentObjectUrisList() {
        return currentObjectUris;
    }

    /**
     * @return ordered set of object URIs to which the distances form the currentQueryUri were evaluated
     */
    public Set<String> getCurrentObjectUrisSet() {
        return new TreeSet<>(currentObjectUris);
    }

    public boolean hasNext() {
        return nextQueryUri != null;
    }

    /**
     * Parses the error output created during query evaluation. Reads all objects to which distances from the last
     * parsed query object were evaluated.
     *
     * @throws IOException propagates the exception
     */
    public void parseNextQueryEvalErrOutput() throws IOException {
        if (nextQueryUri == null)
            return;
        currentQueryUri = nextQueryUri;
        currentObjectUris = new ArrayList<>();
        String line = reader.readLine();
        while (line != null) {
            queryObjectUriMatcher = queryObjectUriPattern.matcher(line);
            if (queryObjectUriMatcher.matches()) {
                nextQueryUri = queryObjectUriMatcher.group(1);
                return;
            }
            Matcher dataObjectUriMatcher = dataObjectUriPattern.matcher(line);
            if (dataObjectUriMatcher.matches()) {
                currentObjectUris.add(dataObjectUriMatcher.group(1).equals(currentQueryUri)
                        && dataObjectUriMatcher.groupCount() > 1 ?
                        dataObjectUriMatcher.group(2) :
                        dataObjectUriMatcher.group(1));
            }
            line = reader.readLine();
        }
        nextQueryUri = null;
    }
}
