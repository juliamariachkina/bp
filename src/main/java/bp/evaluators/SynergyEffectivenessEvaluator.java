package bp.evaluators;

import bp.CSVWriter;
import bp.datasets.DatasetData;
import bp.parsers.GroundTruthParser;
import bp.parsers.ReducedOutputParser;
import bp.parsers.UnfilteredObjectsParser;
import bp.utils.Utility;
import messif.objects.LocalAbstractObject;
import messif.objects.util.AbstractObjectIterator;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.*;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class SynergyEffectivenessEvaluator {
    private static final Logger LOG = Logger.getLogger(SynergyEffectivenessEvaluator.class.getName());

    public static final int MIN_RECALL = 28;

    private final String filePathToStoreResults;
    private final DatasetData datasetData;

    public SynergyEffectivenessEvaluator(String filePathToStoreResults, DatasetData datasetData) {
        this.filePathToStoreResults = filePathToStoreResults;
        this.datasetData = datasetData;
    }

    private int computeCandSetMedian(Map<String, List<String>> queryURItoObjectURIs, Map<String, Set<String>> groundTruth,
                                     AbstractObjectIterator<LocalAbstractObject> pivotIter) {
        int[] candSetSizePerQueryToMeetAccuracy = new int[datasetData.queryCount];
        int index = 0;
        for (Map.Entry<String, List<String>> queryURItoObjectURIsEntry : queryURItoObjectURIs.entrySet()) {
            int correctlyFound = 0, count = 0;
            for (String objectURI : queryURItoObjectURIsEntry.getValue()) {
                if (correctlyFound >= MIN_RECALL)
                    break;
                try {
                    pivotIter.getObjectByLocator(objectURI);
                    continue;
                } catch (NoSuchElementException e) {
                    //do nothing, since this means that the current object isn't pivot
                }
                if (groundTruth.get(queryURItoObjectURIsEntry.getKey()).contains(objectURI))
                    ++correctlyFound;
                ++count;
            }
            if (correctlyFound < MIN_RECALL)
                LOG.warning("The size of a candidate set for a query object " + queryURItoObjectURIsEntry.getKey()
                        + "isn't sufficient, since the recall is " + correctlyFound);
            candSetSizePerQueryToMeetAccuracy[index] = count;
            ++index;
        }
        Arrays.sort(candSetSizePerQueryToMeetAccuracy);
        return candSetSizePerQueryToMeetAccuracy[datasetData.queryCount / 2];
    }

    public void reduceErrOutputFilesToMedianDistComp(String errFilePath) throws IOException {
        Map<String, Set<String>> groundTruth = new GroundTruthParser(datasetData.groundTruthPath, datasetData.queryPattern).parse();
        AbstractObjectIterator<LocalAbstractObject> pivotIter = Utility.getObjectsIterator(datasetData.pivotFilePath, datasetData.objectClass);

        SortedMap<String, List<String>> queryURIsToObjectURIs = new UnfilteredObjectsParser(errFilePath).parse();
        int candSetMedian = computeCandSetMedian(queryURIsToObjectURIs, groundTruth, pivotIter);
        LOG.info("Cand set median " + candSetMedian);
        queryURIsToObjectURIs.forEach((key, value) -> value = value.stream().limit(candSetMedian).collect(Collectors.toList()));

        CSVWriter.writeReducedErrOutput(filePathToStoreResults, queryURIsToObjectURIs);
    }

    public void evaluateSynergyEffectiveness(String[] errFilePaths) throws IOException {
        if (errFilePaths.length < 2)
            return;
        PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(Utility.getOutputStream(filePathToStoreResults))));
        Map<String, Set<String>> groundTruth = new GroundTruthParser(datasetData.groundTruthPath, datasetData.queryPattern).parse();
        List<ReducedOutputParser> parsers = new ArrayList<>();
        for (String errFilePath : errFilePaths) {
            parsers.add(new ReducedOutputParser(errFilePath));
        }
        Set<String> intersectionObjectURIs = new HashSet<>();

        for (int i = 0; i < datasetData.queryCount; ++i) {
            for (ReducedOutputParser parser : parsers)
                parser.parseNextQueryEvalErrOutput();
            String currentQueryURI = parsers.get(0).getCurrentQueryURI();
            if (parsers.stream()
                    .map(ReducedOutputParser::getCurrentQueryURI)
                    .anyMatch(queryURI -> !queryURI.equals(currentQueryURI)))
                throw new IllegalArgumentException("Not all reduced outputs were stored in a sorted order");
            intersectionObjectURIs = parsers.stream()
                    .map(ReducedOutputParser::getCurrentObjectUris)
                    .reduce((a, b) -> {
                        a.retainAll(b);
                        return a;
                    }).get();

            CSVWriter.writeNextQuerySynergyEffectiveness(writer, groundTruth.get(currentQueryURI),
                    currentQueryURI, intersectionObjectURIs);
        }
        writer.flush();
        writer.close();
    }
}
