package bp.evaluators;

import bp.CSVWriter;
import bp.datasets.DatasetData;
import bp.parsers.*;
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
        int index = 0, under28 = 0;
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
            if (correctlyFound < MIN_RECALL) {
                LOG.warning("The size of a candidate set for a query object " + it.getCurrentQueryURI()
                        + " isn't sufficient, since the recall is " + correctlyFound);
                ++under28;
            }
            candSetSizePerQueryToMeetAccuracy[index] = count;
            ++index;
        }
        LOG.info("Number of queries with the recall under 28 -- " + under28);
        Arrays.sort(candSetSizePerQueryToMeetAccuracy);
        return candSetSizePerQueryToMeetAccuracy[datasetData.queryCount / 2];
    }

    public void reduceErrOutputFilesToMedianDistComp(String errFilePath) throws IOException {
        LOG.info("Processing " + errFilePath);
        Map<String, Set<String>> groundTruth = new GroundTruthParser(datasetData.groundTruthPath, datasetData.queryPattern).parse();
        AbstractObjectIterator<LocalAbstractObject> pivotIter = Utility.getObjectsIterator(datasetData.pivotFilePath, datasetData.objectClass);

        int candSetMedian = computeCandSetMedian(errFilePath, groundTruth, pivotIter);
        LOG.info("Cand set median " + candSetMedian);

        CSVWriter.writeReducedErrOutput(errFilePath, candSetMedian, filePathToStoreResults);
    }

    public void evaluateSynergyEffectiveness(String[] errFilePaths) throws IOException {
        if (errFilePaths.length < 2)
            return;
        long[] candSetSizes = new long[datasetData.queryCount];
        int[] recalls = new int[datasetData.queryCount];
        PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(Utility.getOutputStream(filePathToStoreResults))));
        Map<String, Set<String>> groundTruth = new GroundTruthParser(datasetData.groundTruthPath, datasetData.queryPattern).parse();
        List<ErrOutputIterator> iterators = new ArrayList<>();
        for (String errFilePath : errFilePaths) {
            iterators.add(errFilePath.contains("reducedOutput") ?
                    new ReducedOutputIterator(errFilePath) :
                    new UnfilteredObjectsIterator(errFilePath));
        }
        Set<String> intersectionObjectURIs = new HashSet<>();

        for (int i = 0; i < datasetData.queryCount; ++i) {
            for (ErrOutputIterator iterator : iterators)
                iterator.parseNextQueryEvalErrOutput();
            String currentQueryURI = iterators.get(0).getCurrentQueryURI();
            LOG.info("Processing [" + currentQueryURI + "]");
            if (iterators.stream()
                    .map(ErrOutputIterator::getCurrentQueryURI)
                    .anyMatch(queryURI -> !queryURI.equals(currentQueryURI)))
                throw new IllegalArgumentException("Not all reduced outputs were stored in a sorted order");
            intersectionObjectURIs = iterators.stream()
                    .map(ErrOutputIterator::getCurrentObjectUrisSet)
                    .reduce((a, b) -> {
                        a.retainAll(b);
                        return a;
                    }).get();

            CSVWriter.writeNextQuerySynergyEffectiveness(writer, groundTruth.get(currentQueryURI),
                    currentQueryURI, intersectionObjectURIs, candSetSizes, recalls, i);
        }
        Arrays.sort(recalls);
        Arrays.sort(candSetSizes);
        writer.println("Median recall;" + recalls[datasetData.queryCount / 2]);
        writer.println("Median candSet size;" + candSetSizes[datasetData.queryCount / 2]);

        writer.flush();
        writer.close();
    }

    public void evaluateCandSetsDiffs(String synFilePath, String errFilePath) throws IOException {
        PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(Utility.getOutputStream(filePathToStoreResults))));
        ErrOutputIterator synIt = synFilePath.contains("reducedOutput") ?
                new ReducedOutputIterator(synFilePath) :
                (synFilePath.contains("synergy") ?
                        new SynergyIterator(synFilePath) :
                        new UnfilteredObjectsIterator(synFilePath));
        ErrOutputIterator errIt = errFilePath.contains("reducedOutput") ?
                new ReducedOutputIterator(errFilePath) :
                (errFilePath.contains("synergy") ?
                        new SynergyIterator(errFilePath) :
                        new UnfilteredObjectsIterator(errFilePath));
        long[] candSetDiffs = new long[datasetData.queryCount];
        for (int i = 0; i < datasetData.queryCount; ++i) {
            synIt.parseNextQueryEvalErrOutput();
            errIt.parseNextQueryEvalErrOutput();
            String currentQueryURI = synIt.getCurrentQueryURI();
            LOG.info("Processing [" + currentQueryURI + "]");
            if (!errIt.getCurrentQueryURI().equals(currentQueryURI))
                throw new IllegalArgumentException("Not all reduced outputs were stored in a sorted order");

            errIt.getCurrentObjectUrisSet().removeAll(synIt.getCurrentObjectUrisSet());

            CSVWriter.writeNextQueryCandSetDiffs(writer, currentQueryURI, errIt.getCurrentObjectUrisSet(), candSetDiffs, i);
        }
        Arrays.sort(candSetDiffs);
        writer.println("Cand set reduction (counts objects from errFilePath candSet per query, " +
                "which aren't present in synFilePath candSet for the same query);" + candSetDiffs[datasetData.queryCount / 2]);

        writer.flush();
        writer.close();
    }
}
