package bp.evaluators;

import bp.CSVWriter;
import bp.utils.Utility;
import messif.algorithms.Algorithm;
import messif.algorithms.AlgorithmMethodException;
import messif.objects.LocalAbstractObject;
import messif.objects.util.RankedAbstractObject;
import messif.operations.AnswerType;
import messif.operations.data.BulkInsertOperation;
import messif.operations.query.KNNQueryOperation;
import messif.statistics.StatisticCounter;
import messif.statistics.Statistics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * SimpleQueryEvaluator evaluates m queries on n objects using kNNQueryOperation. All the parameters
 * (including m, n, k, specific algorithm, etc.) are provided in the constructor.
 *
 * @param <T> Any class that extends LocalAbstractObject can be used for
 *           representation of similarity query and data objects in this class
 */
public class SimilarityQueryEvaluator<T extends LocalAbstractObject> {

    private final Algorithm algorithm;
    private final int k;
    private final String dataFilePath;
    private final int dataObjectsCount;
    private final Class<T> dataClass;
    private final List<T> queryObjects;

    private static final Logger LOG = Logger.getLogger(SimilarityQueryEvaluator.class.getName());

    public SimilarityQueryEvaluator(Algorithm algorithm, String queryFilePath, int queryCount,
                                int k, String dataFilePath, int dataObjectsCount, Class<T> dataClass) {
        this.dataClass = dataClass;
        queryObjects = Utility.getObjectsList(queryFilePath, dataClass, queryCount);
        this.algorithm = algorithm;
        this.k = k;
        this.dataFilePath = dataFilePath;
        this.dataObjectsCount = dataObjectsCount;
    }

    public void storeToFile(String path) throws IOException {
        algorithm.storeToFile(path);
    }

    public void insertData() {
        try {
            algorithm.executeOperation(new BulkInsertOperation(
                    Utility.getObjectsIterator(dataFilePath, dataClass),
                    dataObjectsCount));
        } catch (AlgorithmMethodException | NoSuchMethodException e) {
            LOG.log(Level.SEVERE, "Reading objects ended with a failure: " + e.getMessage());
        }
    }

    public void evaluateQueriesAndWriteResult(String filePathToResults, String groundTruthPath, String queryPattern)
            throws IOException {
        Map<String, Long> locatorToDistComp = new HashMap<>();
        Map<String, List<RankedAbstractObject>> result = evaluateQueries(locatorToDistComp);

        CSVWriter writer = new CSVWriter(filePathToResults, groundTruthPath, queryPattern);
        writer.writeQueryResults(result, locatorToDistComp);
    }

    public Map<String, List<RankedAbstractObject>> evaluateQueries(Map<String, Long> locatorToDistComp) {
        Map<String, List<RankedAbstractObject>> result = new HashMap<>();
        try {
            for (LocalAbstractObject queryObject : queryObjects) {
                LOG.log(Level.INFO, "Query object with uri: " + queryObject.getLocatorURI());
                Statistics.resetStatistics();
                KNNQueryOperation op = new KNNQueryOperation(queryObject, k, AnswerType.NODATA_OBJECTS);
                op = algorithm.executeOperation(op);

                result.put(queryObject.getLocatorURI(), new ArrayList<>());
                op.getAnswer().forEachRemaining(result.get(queryObject.getLocatorURI())::add);
                locatorToDistComp.put(queryObject.getLocatorURI(), StatisticCounter.getStatistics("DistanceComputations").getValue());
                LOG.log(Level.INFO, "Distance computations: " + StatisticCounter.getStatistics("DistanceComputations"));
            }
        } catch (NoSuchMethodException | AlgorithmMethodException e) {
            LOG.log(Level.WARNING, "Query evaluation ended with a failure: " + e.getMessage());
        }
        return result;
    }
}
