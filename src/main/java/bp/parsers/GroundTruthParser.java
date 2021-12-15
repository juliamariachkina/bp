package bp.parsers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * This class parses the ground truths of query objects.
 */
public class GroundTruthParser {
    /**
     * File path to the ground truth file
     **/
    private final String filePath;
    /**
     * The pattern of query URI
     **/
    private final String queryUriPattern;

    private static final Logger LOG = Logger.getLogger(GroundTruthParser.class.getName());

    public GroundTruthParser(String filePath, String queryUriPattern) {
        this.filePath = filePath;
        this.queryUriPattern = queryUriPattern;
    }

    /**
     * Parses the ground truth file.
     *
     * @return map with a query URI as key and a set of 30 URIs of its true nearest neighbors as a value
     * @throws IOException
     */
    public Map<String, Set<String>> parse() throws IOException {
        Map<String, Set<String>> result = new HashMap<>();
        BufferedReader reader = new BufferedReader(new FileReader(filePath));

        Pattern queryObjectUriPattern = Pattern.compile(queryUriPattern);
        Matcher queryObjectUriMatcher;

        String lastQueryUri;
        String line = reader.readLine();
        while (line != null) {
            queryObjectUriMatcher = queryObjectUriPattern.matcher(line);
            if (!queryObjectUriMatcher.matches()) {
                line = reader.readLine();
                continue;
            }
            lastQueryUri = queryObjectUriMatcher.group(1);
            line = reader.readLine();
            String finalLastQueryUri = lastQueryUri;
            result.put(queryObjectUriMatcher.group(1),
                    Arrays.stream(line.replaceAll(" ?[^:,]*: ", "").split(","))
                            .limit(30)
                            .collect(Collectors.toSet()));
            line = reader.readLine();
        }
        return result;
    }
}
