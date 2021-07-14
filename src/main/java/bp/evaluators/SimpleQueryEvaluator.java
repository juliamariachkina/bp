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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.StreamSupport;

public class SimpleQueryEvaluator {

    private final Algorithm algorithm;
    private final int k;
    private final String dataFilePath;
    private final int dataObjectsCount;
    private List<LocalAbstractObject> queryObjects;

    private static final Logger LOG = Logger.getLogger(Utility.class.getName());

    public SimpleQueryEvaluator(Algorithm algorithm, String queryFilePath, int queryCount,
                                int k, String dataFilePath, int dataObjectsCount) {
        try {
            queryObjects = Utility.getObjectsList(queryFilePath, LocalAbstractObject.class, queryCount);
        } catch (IOException e) {
            LOG.log(Level.SEVERE, "Reading objects ended with a failure: " + e.getMessage());
        }

        this.algorithm = algorithm;
        this.k = k;
        this.dataFilePath = dataFilePath;
        this.dataObjectsCount = dataObjectsCount;
    }

    public Map<String, List<RankedAbstractObject>> evaluateQueries() {
        Map<String, List<RankedAbstractObject>> result = new HashMap<>();
        try {
            algorithm.executeOperation(new BulkInsertOperation(
                    Utility.getObjectsIterator(dataFilePath, LocalAbstractObject.class),
                    dataObjectsCount));
        } catch (IOException | AlgorithmMethodException | NoSuchMethodException e) {
            LOG.log(Level.SEVERE, "Reading objects ended with a failure: " + e.getMessage());
            return result;
        }
        try {
            for (LocalAbstractObject queryObject : queryObjects) {
                KNNQueryOperation op = new KNNQueryOperation(queryObject, k, AnswerType.ORIGINAL_OBJECTS);
                op = algorithm.executeOperation(op);
                result.put(queryObject.getLocatorURI(), Lists.newArrayList(op.getAnswer()));
            }
        } catch (NoSuchMethodException | AlgorithmMethodException e) {
            LOG.log(Level.WARNING, "Query evaluation ended with a failure: " + e.getMessage());
        }
        return result;
    }
}
