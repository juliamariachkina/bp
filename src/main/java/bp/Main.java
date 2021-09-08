package bp;

import bp.evaluators.SimilarityQueryEvaluator;
import bp.utils.Utility;
import messif.algorithms.Algorithm;
import messif.algorithms.impl.ParallelSequentialScan;
import messif.algorithms.impl.SequentialScan;
import messif.buckets.CapacityFullException;
import messif.buckets.LocalBucket;
import messif.buckets.impl.DiskBlockBucket;
import messif.objects.LocalAbstractObject;
import messif.objects.impl.ObjectFloatVectorL2;
import messif.objects.util.AbstractObjectIterator;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    private static final Logger LOG = Logger.getLogger(Main.class.getName());

    public static void main(String args[])
            throws IOException, CapacityFullException, InstantiationException, ClassNotFoundException {
        createAndStoreLaesaSift();
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
        createAndStoreAlgorithm("../data/sift/pivots_256_randomFromDataset.data.txt", ObjectFloatVectorL2.class,
                DiskBlockBucket.class, 256, "../data/sift/queryset-sift-1000.data.txt",
                1000, 30, "../data/sift/sift-1M.data.txt", 1000000,
                "src/main/java/bp/storedAlgos/laesaSift");
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

    public static <T extends LocalAbstractObject> void restoreAndExecuteQueriesLaesaSift()
            throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries("src/main/java/bp/storedAlgos/laesaSift", ObjectFloatVectorL2.class,
                "../data/sift/queryset-sift-1000.data.txt",
                1000, 30, "../data/sift/sift-1M.data.txt", 1000000,
                "src/main/java/bp/results/LaesaSift.csv");
    }

    public static void prepareAndExecuteSeqScan() throws IOException {
        SimilarityQueryEvaluator<ObjectFloatVectorL2> similarityQueryEvaluator = new SimilarityQueryEvaluator<>(
                new ParallelSequentialScan(4),
                "../data/D20_query_objects_uniform_distribution.data",
                10, 30, "../data/D20_data_objects_uniform_distribution.data",
                100000, ObjectFloatVectorL2.class);
        similarityQueryEvaluator.insertData();

        similarityQueryEvaluator.evaluateQueriesAndWriteResult("src/main/java/bp/results/SeqScan.csv");
    }
}
