package bp.evaluators;

import bp.CSVWriter;
import bp.datasets.DatasetData;
import bp.parsers.GroundTruthParser;
import bp.parsers.ReducedOutputIterator;
import bp.parsers.UnfilteredObjectsIterator;
import bp.utils.Utility;
import messif.objects.LocalAbstractObject;
import messif.objects.util.AbstractObjectIterator;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.*;
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

    private int computeCandSetMedian(String errFilePath, Map<String, Set<String>> groundTruth,
                                     AbstractObjectIterator<LocalAbstractObject> pivotIter) throws IOException {
        int[] candSetSizePerQueryToMeetAccuracy = new int[datasetData.queryCount];
        UnfilteredObjectsIterator it = new UnfilteredObjectsIterator(errFilePath);
        int index = 0;
        while (it.hasNext()) {
            it.parseNextQueryEvalErrOutput();
            LOG.info("Query URI " + it.getCurrentQueryURI() + " object count " + it.getCurrentObjectUrisList().size());
            int correctlyFound = 0, count = 0;
            for (String objectURI : it.getCurrentObjectUrisList()) {
                if (correctlyFound >= MIN_RECALL)
                    break;
                try {
                    pivotIter.getObjectByLocator(objectURI);
                    continue;
                } catch (NoSuchElementException e) {
                    //do nothing, since this means that the current object isn't pivot
                }
                if (groundTruth.get(it.getCurrentQueryURI()).contains(objectURI))
                    ++correctlyFound;
                ++count;
            }
            if (correctlyFound < MIN_RECALL)
                LOG.warning("The size of a candidate set for a query object " + it.getCurrentQueryURI()
                        + " isn't sufficient, since the recall is " + correctlyFound);
            candSetSizePerQueryToMeetAccuracy[index] = count;
            ++index;
        }
        Arrays.sort(candSetSizePerQueryToMeetAccuracy);
        return candSetSizePerQueryToMeetAccuracy[datasetData.queryCount / 2];
    }

    public void reduceErrOutputFilesToMedianDistComp(String errFilePath) throws IOException {
        Map<String, Set<String>> groundTruth = new GroundTruthParser(datasetData.groundTruthPath, datasetData.queryPattern).parse();
        AbstractObjectIterator<LocalAbstractObject> pivotIter = Utility.getObjectsIterator(datasetData.pivotFilePath, datasetData.objectClass);

        int candSetMedian = computeCandSetMedian(errFilePath, groundTruth, pivotIter);
        LOG.info("Cand set median " + candSetMedian);

        CSVWriter.writeReducedErrOutput(errFilePath, candSetMedian, filePathToStoreResults);
    }

    public void evaluateSynergyEffectiveness(String[] errFilePaths) throws IOException {
        if (errFilePaths.length < 2)
            return;
        PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(Utility.getOutputStream(filePathToStoreResults))));
        Map<String, Set<String>> groundTruth = new GroundTruthParser(datasetData.groundTruthPath, datasetData.queryPattern).parse();
        List<ReducedOutputIterator> iterators = new ArrayList<>();
        for (String errFilePath : errFilePaths) {
            iterators.add(new ReducedOutputIterator(errFilePath));
        }
        Set<String> intersectionObjectURIs = new HashSet<>();

        for (int i = 0; i < datasetData.queryCount; ++i) {
            for (ReducedOutputIterator iterator : iterators)
                iterator.parseNextQueryEvalErrOutput();
            String currentQueryURI = iterators.get(0).getCurrentQueryURI();
            LOG.info("Processing " + currentQueryURI);
            if (iterators.stream()
                    .map(ReducedOutputIterator::getCurrentQueryURI)
                    .anyMatch(queryURI -> !queryURI.equals(currentQueryURI)))
                throw new IllegalArgumentException("Not all reduced outputs were stored in a sorted order");
            intersectionObjectURIs = iterators.stream()
                    .map(ReducedOutputIterator::getCurrentObjectUrisSet)
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
