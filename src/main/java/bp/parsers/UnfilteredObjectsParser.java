package bp.parsers;

import bp.utils.Utility;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UnfilteredObjectsParser {
    private final String filePath;

    private static final Logger LOG = Logger.getLogger(UnfilteredObjectsParser.class.getName());

    public UnfilteredObjectsParser(String filePath) {
        this.filePath = filePath;
    }

    public SortedMap<String, List<String>> parse() throws IOException {
        SortedMap<String, List<String>> result = new TreeMap<>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(Utility.openInputStream(filePath)));

        Pattern queryObjectUriPattern = Pattern.compile("INFO: Query object with uri: (.*)");
        Matcher queryObjectUriMatcher;
        Pattern dataObjectUriPattern = Pattern.compile("(.*);(.*)");
        Matcher dataObjectUriMatcher;

        String line = reader.readLine();
        String queryURI = "";
        List<String> computedDistancesTo = new ArrayList<>();
        while (line != null) {
            queryObjectUriMatcher = queryObjectUriPattern.matcher(line);
            if (queryObjectUriMatcher.matches()) {
                if (!queryURI.equals("")) {
                    result.put(queryURI, computedDistancesTo);
                    computedDistancesTo = new ArrayList<>();
                }
                queryURI = queryObjectUriMatcher.group(1);
            }
            dataObjectUriMatcher = dataObjectUriPattern.matcher(line);
            if (dataObjectUriMatcher.matches())
                computedDistancesTo.add(dataObjectUriMatcher.group(1).equals(queryURI) ?
                        dataObjectUriMatcher.group(2) :
                        dataObjectUriMatcher.group(1));
            line = reader.readLine();
        }
        result.put(queryURI, computedDistancesTo);
        return result;
    }
}
