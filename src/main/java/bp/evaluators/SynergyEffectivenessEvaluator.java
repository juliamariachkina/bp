package bp.evaluators;

import bp.CSVWriter;
import bp.datasets.DatasetData;
import bp.parsers.ErrOutputIterator;
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

/**
 * This class evaluates the synergy effectiveness. It has a method to limit the candidate sets identified by a technique
 * to a value so that the median recall over all queries is equal to the MIN_RECALL required. It also contains a
 * method to evaluate the median recall and the median candidate set size of any synergy of techniques.
 */
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

    /**
     * Computes the median candidate set size required for the median recall of an indexing technique over all
     * evaluated queries to be equal to the MIN_RECALL. Then reduces the candidate sets accordingly.
     *
     * @param errFilePath file path to the file where the records of all distance computations between query objects
     *                    and data objects (and pivots) evaluated during query processings are stored.
     * @throws IOException propagates the exception
     */
    public void reduceErrOutputFilesToMedianDistComp(String errFilePath) throws IOException {
        Map<String, Set<String>> groundTruth = new GroundTruthParser(datasetData.groundTruthPath, datasetData.queryPattern).parse();
        AbstractObjectIterator<LocalAbstractObject> pivotIter = Utility.getObjectsIterator(datasetData.pivotFilePath, datasetData.objectClass);

        int candSetMedian = computeCandSetMedian(errFilePath, groundTruth, pivotIter);
        LOG.info("Cand set median " + candSetMedian);

        CSVWriter.writeReducedErrOutput(errFilePath, candSetMedian, filePathToStoreResults);
    }

    /**
     * Intersects candidate sets identified by indexing techniques and stored in files at errFilePaths.
     * Evaluates the size and the recall of each intersected candidate set. Evaluates the median recall and median
     * candidate size set of the synergy over all query objects.
     *
     * @param errFilePaths file path to the file where the records of all distance computations between query objects
     *                     and data objects (and pivots) evaluated during query processings are stored.
     * @throws IOException propagates the exception
     */
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
        writer.println("Median recalls;" + Arrays.toString(recalls));
        writer.println("Median recall;" + recalls[datasetData.queryCount / 2]);
        writer.println("Median candSet size;" + candSetSizes[datasetData.queryCount / 2]);

        writer.flush();
        writer.close();
    }

}
