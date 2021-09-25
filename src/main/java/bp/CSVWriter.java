package bp;

import bp.parsers.GroundTruthParser;
import messif.objects.util.RankedAbstractObject;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

public class CSVWriter {
    private final String filePath;
    private final String groundTruthPath;
    private final String queryPattern;

    private static final Logger LOG = Logger.getLogger(CSVWriter.class.getName());

    public CSVWriter(String filePath, String groundTruthPath, String queryPattern) {
        this.filePath = filePath;
        this.groundTruthPath = groundTruthPath;
        this.queryPattern = queryPattern;
    }

    public void writeQueryResults(Map<String, List<RankedAbstractObject>> results,
                                  Map<String, Long> locatorToDistComp) throws IOException {
        PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(filePath, false)));
        Map<String, Set<String>> queryUriToObjectUris = new GroundTruthParser(groundTruthPath, queryPattern).parse();

        writeAnswer(writer, results, locatorToDistComp, queryUriToObjectUris);
        writer.flush();
    }

    private void writeAnswer(PrintWriter writer,
                             Map<String, List<RankedAbstractObject>> results,
                             Map<String, Long> locatorToDistComp,
                             Map<String, Set<String>> queryUriToObjectUris) {
        for (Map.Entry<String, List<RankedAbstractObject>> locatorToKNN : results.entrySet()) {
            writer.println("IDquery;" + locatorToKNN.getKey());

            for (RankedAbstractObject neighbour : locatorToKNN.getValue()) {
                String isCorrect = queryUriToObjectUris.get(locatorToKNN.getKey())
                        .contains(neighbour.getObject().getLocatorURI()) ? "ok" : "nok";
                writer.println(neighbour.getDistance() + ";" + neighbour.getObject().getLocatorURI() + ";" + isCorrect);
            }

            writer.println("Number of distance computations:" + locatorToDistComp.get(locatorToKNN.getKey()));
        }
    }
}
