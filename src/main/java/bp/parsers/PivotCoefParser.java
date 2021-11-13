package bp.parsers;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PivotCoefParser {
    private final String filePath;

    public PivotCoefParser(String filePath) {
        this.filePath = filePath;
    }

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
