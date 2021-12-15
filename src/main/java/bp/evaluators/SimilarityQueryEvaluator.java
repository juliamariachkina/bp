package bp.evaluators;

import bp.CSVWriter;
import bp.datasets.DatasetData;
import bp.utils.Utility;
import messif.algorithms.Algorithm;
import messif.algorithms.AlgorithmMethodException;
import messif.objects.LocalAbstractObject;
import messif.objects.util.RankedAbstractObject;
import messif.operations.AnswerType;
import messif.operations.Approximate;
import messif.operations.data.BulkInsertOperation;
import messif.operations.query.ApproxKNNQueryOperation;
import messif.operations.query.KNNQueryOperation;
import messif.statistics.StatisticCounter;
import messif.statistics.Statistics;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * SimilarityQueryEvaluator inserts objects to the algorithm and evaluates kNN queries (precise or approximate) on
 * the algorithm.
 *
 * @param <T> Any class that extends LocalAbstractObject can be used for a
 *            representation of a query, data and pivot objects in this class
 */
public class SimilarityQueryEvaluator<T extends LocalAbstractObject> {

    private final Algorithm algorithm;
    private final int k;
    private final DatasetData datasetData;
    private final List<? extends LocalAbstractObject> queryObjects;

    private static final Logger LOG = Logger.getLogger(SimilarityQueryEvaluator.class.getName());

    /**
     * Creates a new SimilarityQueryEvaluator instance.
     * @param algorithm an algorithm to be used for all similarity related operations by this SimilarityQueryEvaluator
     * @param k parameter k for KNNQueryOperation or ApproxKNNQueryOperation
     * @param datasetData metadata of a specific dataset
     */
    public SimilarityQueryEvaluator(Algorithm algorithm, int k, DatasetData datasetData) {
        queryObjects = Utility.getObjectsList(datasetData.queryFilePath, datasetData.objectClass, datasetData.queryCount);
        this.algorithm = algorithm;
        this.k = k;
        this.datasetData = datasetData;
    }

    public void storeToFile(String path) throws IOException {
        algorithm.storeToFile(path);
    }

    public void insertData() {
        try {
            algorithm.executeOperation(new BulkInsertOperation(
                    Utility.getObjectsIterator(datasetData.dataFilePath, datasetData.objectClass),
                    datasetData.dataObjectsCount));
        } catch (AlgorithmMethodException | NoSuchMethodException e) {
            LOG.log(Level.SEVERE, "Reading objects ended with a failure: " + e.getMessage() + "\n"
                    + Arrays.toString(e.getStackTrace()));
        }
    }

    public void evaluateQueriesAndWriteResult(String filePathToResults, String groundTruthPath, String queryPattern,
                                              boolean isApproxOp) throws IOException {
        Map<String, Long> locatorToDistComp = new HashMap<>();
        Map<String, List<RankedAbstractObject>> result = evaluateQueries(locatorToDistComp, isApproxOp);

        CSVWriter writer = new CSVWriter(filePathToResults, groundTruthPath, queryPattern);
        writer.writeQueryResults(result, locatorToDistComp);
    }

    public Map<String, List<RankedAbstractObject>> evaluateQueries(Map<String, Long> locatorToDistComp, boolean isApproxOp) {
        Map<String, List<RankedAbstractObject>> result = new HashMap<>();
        try {
            for (LocalAbstractObject queryObject : queryObjects) {
                LOG.log(Level.INFO, "Query object with uri: " + queryObject.getLocatorURI());
                Statistics.resetStatistics();
                KNNQueryOperation op = isApproxOp ?
                        new ApproxKNNQueryOperation(queryObject, k, AnswerType.NODATA_OBJECTS, 50000,
                                Approximate.LocalSearchType.ABS_OBJ_COUNT, -1) :
                        new KNNQueryOperation(queryObject, k, AnswerType.NODATA_OBJECTS);
                op = algorithm.executeOperation(op);

                result.put(queryObject.getLocatorURI(), new ArrayList<>());
                op.getAnswer().forEachRemaining(result.get(queryObject.getLocatorURI())::add);
                locatorToDistComp.put(queryObject.getLocatorURI(),
                        StatisticCounter.getStatistics("DistanceComputations").getValue());
                LOG.log(Level.INFO, "Distance computations: " + StatisticCounter.getStatistics("DistanceComputations"));
            }
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Query evaluation ended with a failure: " + e.getMessage()
                    + Arrays.toString(e.getStackTrace()));
        }
        return result;
    }
}
