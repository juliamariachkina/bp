package bp.evaluators;

import bp.utils.Utility;
import com.google.common.collect.Lists;
import messif.algorithms.Algorithm;
import messif.algorithms.AlgorithmMethodException;
import messif.objects.LocalAbstractObject;
import messif.objects.util.RankedAbstractObject;
import messif.operations.AnswerType;
import messif.operations.data.BulkInsertOperation;
import messif.operations.query.KNNQueryOperation;

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
public class SimpleQueryEvaluator<T extends LocalAbstractObject> {

    private final Algorithm algorithm;
    private final int k;
    private final String dataFilePath;
    private final int dataObjectsCount;
    private final Class<T> dataClass;
    private final List<T> queryObjects;

    private static final Logger LOG = Logger.getLogger(SimpleQueryEvaluator.class.getName());

    public SimpleQueryEvaluator(Algorithm algorithm, String queryFilePath, int queryCount,
                                int k, String dataFilePath, int dataObjectsCount, Class<T> dataClass) {
        this.dataClass = dataClass;
        queryObjects = Utility.getObjectsList(queryFilePath, dataClass, queryCount);
        this.algorithm = algorithm;
        this.k = k;
        this.dataFilePath = dataFilePath;
        this.dataObjectsCount = dataObjectsCount;
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

    public Map<String, List<RankedAbstractObject>> evaluateQueries() {
        Map<String, List<RankedAbstractObject>> result = new HashMap<>();
        try {
            for (LocalAbstractObject queryObject : queryObjects) {
                KNNQueryOperation op = new KNNQueryOperation(queryObject, k, AnswerType.NODATA_OBJECTS);
                op = algorithm.executeOperation(op);
                result.put(queryObject.getLocatorURI(), Lists.newArrayList(op.getAnswer()));
            }
        } catch (NoSuchMethodException | AlgorithmMethodException e) {
            LOG.log(Level.WARNING, "Query evaluation ended with a failure: " + e.getMessage());
        }
        return result;
    }
}
