package bp;

import messif.objects.util.RankedAbstractObject;
import messif.statistics.StatisticCounter;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class CSVWriter {
    private final String filePath;

    private static final Logger LOG = Logger.getLogger(CSVWriter.class.getName());

    public CSVWriter(String filePath) {
        this.filePath = filePath;
    }

    public void writeQueryResults(Map<String, List<RankedAbstractObject>> results, StatisticCounter distComp) throws IOException {
        PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(filePath, false)));
        writeAnswer(writer, results);
        writeDistanceComputations(writer, distComp);
        writer.flush();
    }

    private void writeAnswer(PrintWriter writer, Map<String, List<RankedAbstractObject>> results) {
        for (Map.Entry<String, List<RankedAbstractObject>> locatorTokNN : results.entrySet()) {
            writer.println("IDquery;" + locatorTokNN.getKey());
            for (RankedAbstractObject neighbour : locatorTokNN.getValue()) {
                writer.println(neighbour.getDistance() + ";" + neighbour.getObject().getLocatorURI());
            }
        }
    }

    private void writeDistanceComputations(PrintWriter writer, StatisticCounter distComp) {
        writer.println("Number of distance computations:" + distComp.getValue());
    }
}
