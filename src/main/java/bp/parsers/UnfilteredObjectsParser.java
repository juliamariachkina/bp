package bp.parsers;

import jdk.nashorn.internal.runtime.regexp.joni.Regex;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UnfilteredObjectsParser {
    private final String filePath;

    private static final Logger LOG = Logger.getLogger(UnfilteredObjectsParser.class.getName());

    public UnfilteredObjectsParser(String filePath) {
        this.filePath = filePath;
    }

    public Map<String, Set<String>> parse() throws IOException {
        Map<String, Set<String>> result = new HashMap<>();
        BufferedReader reader = new BufferedReader(new FileReader(filePath));

        Pattern queryObjectUriPattern = Pattern.compile("INFO: Query object with uri: (.*)");
        Matcher queryObjectUriMatcher;
        Pattern dataObjectUriPattern = Pattern.compile("(.*);(.*)");
        Matcher dataObjectUriMatcher;

        String line = reader.readLine();
        while (line != null) {
            queryObjectUriMatcher = queryObjectUriPattern.matcher(line);
            if (queryObjectUriMatcher.matches())
                result.put(queryObjectUriMatcher.group(1), new HashSet<>());
            dataObjectUriMatcher = dataObjectUriPattern.matcher(line);
            if (dataObjectUriMatcher.matches())
                result.get(dataObjectUriMatcher.group(2)).add(dataObjectUriMatcher.group(1));
            line = reader.readLine();
        }
        return result;
    }
}
