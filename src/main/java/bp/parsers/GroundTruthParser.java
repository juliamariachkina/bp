package bp.parsers;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class GroundTruthParser {
    private final String filePath;
    private final String queryUriPattern;

    private static final Logger LOG = Logger.getLogger(GroundTruthParser.class.getName());

    public GroundTruthParser(String filePath, String queryUriPattern) {
        this.filePath = filePath;
        this.queryUriPattern = queryUriPattern;
    }

    public Map<String, Set<String>> parse() throws IOException {
        Map<String, Set<String>> result = new HashMap<>();
        BufferedReader reader = new BufferedReader(new FileReader(filePath));

        Pattern queryObjectUriPattern = Pattern.compile(queryUriPattern);
        Matcher queryObjectUriMatcher;
        Pattern dataObjectUriPattern = Pattern.compile("\\d+\\.\\d+: .*");
        Matcher dataObjectUriMatcher;

        String lastQueryUri = null;
        String line = reader.readLine();
        while (line != null) {
            //LOG.log(Level.INFO, line);
            queryObjectUriMatcher = queryObjectUriPattern.matcher(line);
            if (queryObjectUriMatcher.matches()) {
                //LOG.log(Level.WARNING, queryObjectUriMatcher.group(1));
                lastQueryUri = queryObjectUriMatcher.group(1);
                result.put(queryObjectUriMatcher.group(1), new HashSet<>());
            }
            dataObjectUriMatcher = dataObjectUriPattern.matcher(line);
            if (dataObjectUriMatcher.matches()) {
                //LOG.log(Level.WARNING, dataObjectUriMatcher.group());
                List<String> neighbours = Arrays.stream(dataObjectUriMatcher.group()
                        .replaceAll(" ?\\d+\\.\\d+: ", "")
                        .split(","))
                        .collect(Collectors.toList());
                result.get(lastQueryUri).addAll(neighbours);
            }
            line = reader.readLine();
        }
        return result;

    }
}
