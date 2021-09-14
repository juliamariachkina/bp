package bp;

import bp.evaluators.SimilarityQueryEvaluator;
import bp.utils.Utility;
import messif.algorithms.Algorithm;
import messif.algorithms.impl.ParallelSequentialScan;
import messif.algorithms.impl.SequentialScan;
import messif.buckets.CapacityFullException;
import messif.buckets.LocalBucket;
import messif.buckets.impl.DiskBlockBucket;
import messif.buckets.impl.MemoryStorageBucket;
import messif.objects.LocalAbstractObject;
import messif.objects.impl.MetaObjectSAPIRWeightedDist2;
import messif.objects.impl.ObjectFloatVectorL2;
import messif.objects.util.AbstractObjectIterator;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    private static final Logger LOG = Logger.getLogger(Main.class.getName());

    public static void main(String args[])
            throws IOException, CapacityFullException, InstantiationException, ClassNotFoundException {
        createAndStoreLaesaDecaf();
    }

    public static <T extends LocalAbstractObject> void createAndStoreAlgorithm(String pivotFilePath, Class<T> objectClass,
                                                                               Class<? extends LocalBucket> bucketClass, int pivotCount,
                                                                               String queryFilePath, int queryCount, int k, String dataFilePath,
                                                                               int dataObjectsCount, String filePathToStoreAlgo)
            throws CapacityFullException, InstantiationException, IOException {
        LOG.log(Level.INFO, "Create and Store algorithm method starts");
        AbstractObjectIterator<LocalAbstractObject> pivotIter = Utility.getObjectsIterator(pivotFilePath, objectClass);
        LOG.log(Level.INFO, "Pivot iterator created");
        SequentialScan laesa = new SequentialScan(bucketClass, pivotIter, pivotCount, true);
        LOG.log(Level.INFO, "Laesa initialised");

        SimilarityQueryEvaluator<T> similarityQueryEvaluator = new SimilarityQueryEvaluator<>(
                laesa, queryFilePath, queryCount, k, dataFilePath, dataObjectsCount, objectClass);
        similarityQueryEvaluator.insertData();
        LOG.log(Level.INFO, "Data objects inserted");

        similarityQueryEvaluator.storeToFile(filePathToStoreAlgo);
        LOG.log(Level.INFO, "Algorithm stored to a file");
    }

    public static void createAndStoreLaesaSift() throws CapacityFullException, IOException, InstantiationException {
        createAndStoreAlgorithm("../sift/pivots_256", ObjectFloatVectorL2.class,
                DiskBlockBucket.class, 256, "../sift/query_1000",
                1000, 30, "../sift/data_1M", 1000000,
                "src/main/java/bp/storedAlgos/laesaSift");
    }

    public static void createAndStoreLaesaRandom() throws CapacityFullException, IOException, InstantiationException {
        createAndStoreAlgorithm("../D20_pivot_objects_uniform_distribution.data", ObjectFloatVectorL2.class,
                MemoryStorageBucket.class, 256, "../D20_query_objects_uniform_distribution.data",
                1000, 30, "../D20_data_objects_uniform_distribution.data", 100000,
                "src/main/java/bp/storedAlgos/laesaRandom");
    }

    public static void createAndStoreLaesaDecaf() throws CapacityFullException, IOException, InstantiationException {
        createAndStoreAlgorithm("../decaf/pivots_2560", ObjectFloatVectorL2.class,
                DiskBlockBucket.class, 256, "../decaf/query_1000",
                1000, 30, "../decaf/data_1M", 1000000,
                "src/main/java/bp/storedAlgos/laesaDecaf");
    }

    public static void createAndStoreLaesaMpeg() throws CapacityFullException, IOException, InstantiationException {
        createAndStoreAlgorithm("../mpeg/pivots_2560", MetaObjectSAPIRWeightedDist2.class,
                DiskBlockBucket.class, 256, "../mpeg/query_1000",
                1000, 30, "../mpeg/data_1M", 1000000,
                "src/main/java/bp/storedAlgos/laesaMpeg");
    }

    public static <T extends LocalAbstractObject> void restoreAndExecuteQueries(String algoFilePath, Class<T> objectClass,
                                                                                String queryFilePath, int queryCount,
                                                                                int k, String dataFilePath,
                                                                                int dataObjectsCount, String filePathToStoreResults)
            throws IOException, ClassNotFoundException {
        SimilarityQueryEvaluator<T> similarityQueryEvaluator = new SimilarityQueryEvaluator<>(
                Algorithm.restoreFromFile(algoFilePath), queryFilePath, queryCount,
                k, dataFilePath, dataObjectsCount, objectClass);

        similarityQueryEvaluator.evaluateQueriesAndWriteResult(filePathToStoreResults);
    }

    public static void restoreAndExecuteQueriesLaesaSift() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries("src/main/java/bp/storedAlgos/laesaSift", ObjectFloatVectorL2.class,
                "../sift/query_1000",
                1000, 30, "../sift/data_1M", 1000000,
                "src/main/java/bp/results/LaesaSift.csv");
    }

    public static void restoreAndExecuteQueriesLaesaRandom() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries("src/main/java/bp/storedAlgos/laesaRandom", ObjectFloatVectorL2.class,
                "../D20_query_objects_uniform_distribution.data", 1000, 30,
                "../D20_data_objects_uniform_distribution.data",
                100000, "src/main/java/bp/results/LaesaRandom.csv");
    }

    public static void restoreAndExecuteQueriesLaesaDecaf() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries("src/main/java/bp/storedAlgos/laesaDecaf", ObjectFloatVectorL2.class,
                "../decaf/query_1000", 1000, 30,
                "../decaf/data_1M", 1000000,
                "src/main/java/bp/results/LaesaDecaf.csv");
    }

    public static void restoreAndExecuteQueriesLaesaMpeg() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries("src/main/java/bp/storedAlgos/laesaMpeg", MetaObjectSAPIRWeightedDist2.class,
                "../mpeg/query_1000",
                1000, 30, "../mpeg/data_1M", 1000000,
                "src/main/java/bp/results/LaesaMpeg.csv");
    }

    public static void prepareAndExecuteSeqScan() throws IOException {
        SimilarityQueryEvaluator<ObjectFloatVectorL2> similarityQueryEvaluator = new SimilarityQueryEvaluator<>(
                new ParallelSequentialScan(4),
                "../D20_query_objects_uniform_distribution.data",
                10, 30, "../D20_data_objects_uniform_distribution.data",
                100000, ObjectFloatVectorL2.class);
        similarityQueryEvaluator.insertData();

        similarityQueryEvaluator.evaluateQueriesAndWriteResult("src/main/java/bp/results/SeqScan.csv");
    }
}
