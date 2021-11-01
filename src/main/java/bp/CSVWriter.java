package bp;

import bp.datasets.DatasetData;
import bp.parsers.GroundTruthParser;
import bp.utils.Utility;
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

    public static void writeSynergyEffectiveness(Map<String, Set<String>> groundTruth, String filePath,
                                                 Map<String, Set<String>> queryURIsToIntersectionObjectURIs) {
        groundTruth.forEach((key, value) -> value.retainAll(queryURIsToIntersectionObjectURIs.get(key)));
        groundTruth.forEach((key, value) -> LOG.info("Query " + key + " values count " + value.size()));

        PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(Utility.getOutputStream(filePath))));
        for (Map.Entry<String, Set<String>> queryURItoFoundTrueNNs : groundTruth.entrySet()) {
            writer.println("IDquery;" + queryURItoFoundTrueNNs.getKey());
            queryURIsToIntersectionObjectURIs.get(queryURItoFoundTrueNNs.getKey()).forEach(writer::println);
            writer.println("Recall;" + queryURItoFoundTrueNNs.getValue().size());
        }
        writer.flush();
        writer.close();
    }

    public static void writeReducedErrOutput(String filePath, Map<String, List<String>> queryURIsToObjectURIs) {
        PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(Utility.getOutputStream(filePath))));
        for (Map.Entry<String, List<String>> queryURItoObjectURIs : queryURIsToObjectURIs.entrySet()) {
            writer.println("IDquery;" + queryURItoObjectURIs.getKey());
            queryURItoObjectURIs.getValue().forEach(writer::println);
        }
        writer.flush();
        writer.close();
    }

    public static void writePivotCoefs(Map<String, Float> pivotURItoCoef, String filePath) throws IOException {
        PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(filePath, false)));
        writePivotCoefs(writer, pivotURItoCoef);
        writer.flush();
    }

    private static void writePivotCoefs(PrintWriter writer, Map<String, Float> pivotURItoCoef) {
        writer.println("Pivot URI;coef");
        for (Map.Entry<String, Float> pivotURIToCoefEntry : pivotURItoCoef.entrySet()) {
            writer.println(pivotURIToCoefEntry.getKey() + ";" + pivotURIToCoefEntry.getValue());
        }
    }

    private void writeAnswer(PrintWriter writer,
                             Map<String, List<RankedAbstractObject>> results,
                             Map<String, Long> locatorToDistComp,
                             Map<String, Set<String>> queryUriToObjectUris) {
        for (Map.Entry<String, List<RankedAbstractObject>> locatorToKNN : results.entrySet()) {
            writer.println("IDquery;" + locatorToKNN.getKey());

            for (RankedAbstractObject neighbour : locatorToKNN.getValue()) {
                Set<String> objectUris = queryUriToObjectUris.get(locatorToKNN.getKey());
                String isCorrect = objectUris == null ?
                        "" :
                        objectUris.contains(neighbour.getObject().getLocatorURI()) ?
                                "ok" :
                                "nok";
                writer.println(neighbour.getDistance() + ";" + neighbour.getObject().getLocatorURI() + ";" + isCorrect);
            }
            writer.println("Number of distance computations:" + locatorToDistComp.get(locatorToKNN.getKey()));
        }
    }
}
