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
    private final String filePath;

    public ReducedOutputParser(String filePath) {
        this.filePath = filePath;
    }

    public Map<String, Set<String>> parse() throws IOException {
        Map<String, Set<String>> result = new HashMap<>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(Utility.openInputStream(filePath)));

        Pattern queryObjectUriPattern = Pattern.compile("IDquery;(.*)");
        Matcher queryObjectUriMatcher;
        Pattern dataObjectUriPattern = Pattern.compile("(.*)");
        Matcher dataObjectUriMatcher;

        String line = reader.readLine();
        String queryURI = "";
        Set<String> computedDistancesTo = new HashSet<>();
        while (line != null) {
            queryObjectUriMatcher = queryObjectUriPattern.matcher(line);
            if (queryObjectUriMatcher.matches()) {
                if (!queryURI.equals("")) {
                    result.put(queryURI, computedDistancesTo);
                    computedDistancesTo = new HashSet<>();
                }
                queryURI = queryObjectUriMatcher.group(1);
            }
            dataObjectUriMatcher = dataObjectUriPattern.matcher(line);
            if (dataObjectUriMatcher.matches())
                computedDistancesTo.add(dataObjectUriMatcher.group(1));
            line = reader.readLine();
        }
        return result;
    }
}
