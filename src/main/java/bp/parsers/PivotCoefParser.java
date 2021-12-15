package bp.parsers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class parses the pivot coefficients, which were created and stored by the PivotCoefs class.
 */
public class PivotCoefParser {
    private final String filePath;

    /**
     * Creates a new instance of a PivotCoefParser.
     *
     * @param filePath file path to the file where the pivot coefficients are stored
     */
    public PivotCoefParser(String filePath) {
        this.filePath = filePath;
    }

    /**
     * Reads and parses the pivot coefficients form a file.
     *
     * @return map with a pivot URI as a key and a coefficient as a value
     * @throws IOException
     */
    public Map<String, Float> parse() throws IOException {
        Map<String, Float> result = new HashMap<>();
        BufferedReader reader = new BufferedReader(new FileReader(filePath));

        Pattern pivotCoefPattern = Pattern.compile("(^(?!Pivot URI).*);(.*)");
        Matcher pivotCoefMatcher;

        String line = reader.readLine();
        while (line != null) {
            pivotCoefMatcher = pivotCoefPattern.matcher(line);
            if (pivotCoefMatcher.matches())
                result.put(pivotCoefMatcher.group(1), Float.parseFloat(pivotCoefMatcher.group(2)));
            line = reader.readLine();
        }
        return result;
    }
}
