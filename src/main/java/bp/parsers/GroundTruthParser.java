package bp.parsers;

import bp.datasets.DecafData;
import bp.datasets.MpegData;
import bp.datasets.RandomData;
import bp.datasets.SiftData;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
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
                            .filter(str -> !str.equals(finalLastQueryUri))
                            .limit(30)
                            .collect(Collectors.toSet()));
            line = reader.readLine();
        }
        return result;
    }
}
