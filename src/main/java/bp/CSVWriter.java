package bp;

import messif.objects.util.RankedAbstractObject;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class CSVWriter {
    private final String filePath;

    private static final Logger LOG = Logger.getLogger(CSVWriter.class.getName());

    public CSVWriter(String filePath) {
        this.filePath = filePath;
    }

    public void writeQueryResults(Map<String, List<RankedAbstractObject>> results,
                                  Map<String, Long> locatorToDistComp)
            throws IOException {
        PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(filePath, false)));
        writeAnswer(writer, results, locatorToDistComp);
        writer.flush();
    }

    private void writeAnswer(PrintWriter writer,
                             Map<String, List<RankedAbstractObject>> results,
                             Map<String, Long> locatorToDistComp) {
        for (Map.Entry<String, List<RankedAbstractObject>> locatorToKNN : results.entrySet()) {
            writer.println("IDquery;" + locatorToKNN.getKey());
            for (RankedAbstractObject neighbour : locatorToKNN.getValue()) {
                writer.println(neighbour.getDistance() + ";" + neighbour.getObject().getLocatorURI());
            }
            writer.println("Number of distance computations:" + locatorToDistComp.get(locatorToKNN.getKey()));
        }
    }
}
