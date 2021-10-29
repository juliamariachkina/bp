package bp.evaluators;

import bp.CSVWriter;
import bp.datasets.DatasetData;
import bp.parsers.GroundTruthParser;
import bp.parsers.UnfilteredObjectsParser;
import bp.utils.Utility;
import messif.objects.LocalAbstractObject;
import messif.objects.util.AbstractObjectIterator;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class SynergyEffectivenessEvaluator {
    private static final Logger LOG = Logger.getLogger(SynergyEffectivenessEvaluator.class.getName());

    public static final int MIN_RECALL = 28;

    private final String filePathToStoreResults;
    private final DatasetData datasetData;
    private final String[] errFilePaths;

    public SynergyEffectivenessEvaluator(String filePathToStoreResults, DatasetData datasetData, String[] errFilePaths) {
        this.filePathToStoreResults = filePathToStoreResults;
        this.datasetData = datasetData;
        this.errFilePaths = errFilePaths;
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

    public void evaluateSynergyEffectiveness() throws IOException {
        List<Map<String, List<String>>> queryURIsToObjectURIsList = new ArrayList<>();
        for (String errFilePath : errFilePaths) {
            queryURIsToObjectURIsList.add(new UnfilteredObjectsParser(errFilePath).parse());
        }

        Map<String, Set<String>> groundTruth = new GroundTruthParser(datasetData.groundTruthPath, datasetData.queryPattern).parse();
        AbstractObjectIterator<LocalAbstractObject> pivotIter = Utility.getObjectsIterator(datasetData.pivotFilePath, datasetData.objectClass);
        queryURIsToObjectURIsList.forEach(queryURIsToObjectURIs -> {
            int candSetMedian = computeCandSetMedian(queryURIsToObjectURIs, groundTruth, pivotIter);
            queryURIsToObjectURIs.forEach((key, value) -> value = value.stream().limit(candSetMedian).collect(Collectors.toList()));
        });

        Map<String, List<String>> queryURIsToIntersectionObjectURIs = queryURIsToObjectURIsList.stream().reduce(
                (a, b) -> {
                    a.forEach((key, value) -> value.retainAll(b.get(key)));
                    return a;
                }).get();

        CSVWriter.writeSynergyEffectiveness(groundTruth, filePathToStoreResults, queryURIsToIntersectionObjectURIs);
    }
}
