package bp;

import bp.datasets.*;
import bp.evaluators.SimilarityQueryEvaluator;
import bp.utils.Utility;
import bp.utils.filteringCoefs.PivotCoefs;
import messif.algorithms.Algorithm;
import messif.algorithms.AlgorithmMethodException;
import messif.algorithms.impl.ParallelSequentialScan;
import messif.algorithms.impl.SequentialScan;
import messif.buckets.CapacityFullException;
import messif.objects.LocalAbstractObject;
import messif.objects.impl.ObjectFloatVectorL2;
import messif.objects.util.AbstractObjectIterator;
import mindex.algorithms.MIndexAlgorithm;
import mtree.MTree;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    private static final Logger LOG = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws IOException, CapacityFullException, AlgorithmMethodException, InstantiationException, ClassNotFoundException {
//        new PivotCoefs(new RandomData()).computePivotCoefs("src/main/java/bp/computedPivotCoefs/Random.csv");
//        new PivotCoefs(new MpegData()).computePivotCoefs("src/main/java/bp/computedPivotCoefs/Mpeg.csv");
//        new PivotCoefs(new DecafData()).computePivotCoefs("src/main/java/bp/computedPivotCoefs/Decaf.csv");
//        createAndStoreLaesaRandom();
//        createAndStoreLaesaMpeg();
//        createAndStoreLaesaDecaf();
//        createAndStoreMIndexRandom();
//        createAndStoreMIndexMpeg();
//        createAndStoreMIndexDecaf();

//        System.setErr(new PrintStream(new FileOutputStream("src/main/java/bp/errorOutputs/laesa/Random.txt")));
//        restoreAndExecuteQueriesLaesaRandom();
//        System.setErr(new PrintStream(new FileOutputStream("src/main/java/bp/errorOutputs/laesa/Mpeg.txt")));
//        restoreAndExecuteQueriesLaesaMpeg();
//        System.setErr(new PrintStream(new FileOutputStream("src/main/java/bp/errorOutputs/laesa/Decaf.txt")));
//        restoreAndExecuteQueriesLaesaDecaf();
//        System.setErr(new PrintStream(new FileOutputStream("src/main/java/bp/errorOutputs/mindex/Random.txt")));
//        restoreAndExecuteQueriesMIndexRandom();
//        System.setErr(new PrintStream(new FileOutputStream("src/main/java/bp/errorOutputs/mindex/Mpeg.txt")));
//        restoreAndExecuteQueriesMIndexMpeg();
//        System.setErr(new PrintStream(new FileOutputStream("src/main/java/bp/errorOutputs/mindex/Decaf.txt")));
//        restoreAndExecuteQueriesMIndexDecaf();

//        new PivotCoefs(new SiftData()).computePivotCoefs("src/main/java/bp/computedPivotCoefs/Sift.csv");
//        createAndStoreMIndexSift();
//        createAndStoreLaesaSift();
//        System.setErr(new PrintStream(new FileOutputStream("src/main/java/bp/errorOutputs/laesa/Sift.txt")));
//        restoreAndExecuteQueriesLaesaSift();
//        System.setErr(new PrintStream(new FileOutputStream("src/main/java/bp/errorOutputs/mindex/Sift.txt")));
//        restoreAndExecuteQueriesMIndexSift();

        createAndStoreMTreeRandom();
        createAndStoreMTreeSift();
//        createAndStoreMTreeDecaf();
        createAndStoreMTreeMpeg();

//        System.setErr(new PrintStream(new FileOutputStream("src/main/java/bp/errorOutputs/mtree/Random.txt")));
//        restoreAndExecuteQueriesMTreeRandom();
//        System.setErr(new PrintStream(new FileOutputStream("src/main/java/bp/errorOutputs/mtree/Sift.txt")));
//        restoreAndExecuteQueriesMTreeSift();
//        System.setErr(new PrintStream(new FileOutputStream("src/main/java/bp/errorOutputs/mtree/Decaf.txt")));
//        restoreAndExecuteQueriesMTreeDecaf();
//        System.setErr(new PrintStream(new FileOutputStream("src/main/java/bp/errorOutputs/mtree/Mpeg.txt")));
//        restoreAndExecuteQueriesMTreeMpeg();
    }

    public static void createAndStoreAlgorithm(DatasetData datasetData, Class<? extends Algorithm> algorithmClass,
                                               int k, String filePathToStoreAlgo)
            throws CapacityFullException, InstantiationException, IOException, AlgorithmMethodException {
        LOG.log(Level.INFO, "Create and Store algorithm method starts");
        AbstractObjectIterator<LocalAbstractObject> pivotIter = Utility.getObjectsIterator(datasetData.pivotFilePath, datasetData.objectClass);
        LOG.log(Level.INFO, "Pivot iterator created");

        Algorithm algorithm;
        if (algorithmClass.equals(SequentialScan.class))
            algorithm = new SequentialScan(datasetData.bucketClass, pivotIter, datasetData.pivotCount, true);
        else {
            if (algorithmClass.equals(MIndexAlgorithm.class))
                algorithm = createMIndex(datasetData);
            else {
                int internalNodeCapacity = 50 * Utility.getObjectsList(datasetData.pivotFilePath, datasetData.objectClass, 1).get(0).getSize();
                algorithm = new MTree(internalNodeCapacity, internalNodeCapacity * 4L, datasetData.pivotCount,
                        pivotIter, datasetData.pivotCount, datasetData.pivotCount);
            }
        }
        LOG.log(Level.INFO, "Algorithm initialised");

        SimilarityQueryEvaluator<? extends LocalAbstractObject> similarityQueryEvaluator = new SimilarityQueryEvaluator<>(
                algorithm, datasetData.queryFilePath, datasetData.queryCount, k,
                datasetData.dataFilePath, datasetData.dataObjectsCount, datasetData.objectClass);
        similarityQueryEvaluator.insertData();
        LOG.log(Level.INFO, "Data objects inserted");

        similarityQueryEvaluator.storeToFile(filePathToStoreAlgo);
        LOG.log(Level.INFO, "Algorithm stored to a file");
    }

    private static MIndexAlgorithm createMIndex(DatasetData datasetData) throws AlgorithmMethodException, InstantiationException {
        Properties props = new Properties();
        props.put("mindex.object_class", datasetData.objectClass.getName());
        props.put("mindex.pivot.number", Integer.toString(datasetData.pivotCount));
        props.put("mindex.minlevel", "1");
        props.put("mindex.maxlevel", "8");
        props.put("mindex.ppcalculator.threads", "1");
        props.put("mindex.use_existing_pivot_permutation", "true");
        props.put("mindex.use_pivot_filtering", "false");
        props.put("mindex.check_duplicate_dc", "false");
        props.put("mindex.max_object_number", "0");
        props.put("mindex.bucket.capacity", "2048");
        props.put("mindex.bucket.min_occupation", "0");
        props.put("mindex.bucket.occupation_as_bytes", "false");
        props.put("mindex.bucket.agile_split", "false");
        props.put("mindex.bucket.class", datasetData.bucketClass.getName());
        props.put("mindex.precise.search", "false");
        props.put("mindex.approximate.force_default", "false");
        props.put("mindex.approximate.type", "ABS_OBJ_COUNT");
        props.put("mindex.approximate.process_whole_multi_bucket", "true");
        props.put("mindex.approximate.check_key_interval", "false");
        props.put("mindex.remove_duplicates", "false");
        props.put("mindex.overfilled.bucket.special", "false");
        props.put("mindex.pivot.file", datasetData.pivotFilePath);

        return new MIndexAlgorithm(props, "mindex.");
    }

    /*------------------------------------------------LAESA---------------------------------------------------------*/

    public static void createAndStoreLaesaSift() throws CapacityFullException, IOException, InstantiationException, AlgorithmMethodException {
        createAndStoreAlgorithm(new SiftData(), SequentialScan.class, 30, "src/main/java/bp/storedAlgos/laesa/Sift");
    }

    public static void createAndStoreLaesaRandom() throws CapacityFullException, IOException, InstantiationException, AlgorithmMethodException {
        createAndStoreAlgorithm(new RandomData(), SequentialScan.class, 30, "src/main/java/bp/storedAlgos/laesa/Random");
    }

    public static void createAndStoreLaesaDecaf() throws CapacityFullException, IOException, InstantiationException, AlgorithmMethodException {
        createAndStoreAlgorithm(new DecafData(), SequentialScan.class, 30, "src/main/java/bp/storedAlgos/laesa/Decaf");
    }

    public static void createAndStoreLaesaMpeg() throws CapacityFullException, IOException, InstantiationException, AlgorithmMethodException {
        createAndStoreAlgorithm(new MpegData(), SequentialScan.class, 30, "src/main/java/bp/storedAlgos/laesa/Mpeg");
    }

    /*------------------------------------------------M-tree---------------------------------------------------------*/

    public static void createAndStoreMTreeSift() throws CapacityFullException, IOException, InstantiationException, AlgorithmMethodException {
        createAndStoreAlgorithm(new SiftData(), MTree.class, 30, "src/main/java/bp/storedAlgos/mtree/Sift");
    }

    public static void createAndStoreMTreeRandom() throws CapacityFullException, IOException, InstantiationException, AlgorithmMethodException {
        createAndStoreAlgorithm(new RandomData(), MTree.class, 30, "src/main/java/bp/storedAlgos/mtree/Random");
    }

    public static void createAndStoreMTreeDecaf() throws CapacityFullException, IOException, InstantiationException, AlgorithmMethodException {
        createAndStoreAlgorithm(new DecafData(), MTree.class, 30, "src/main/java/bp/storedAlgos/mtree/Decaf");
    }

    public static void createAndStoreMTreeMpeg() throws CapacityFullException, IOException, InstantiationException, AlgorithmMethodException {
        createAndStoreAlgorithm(new MpegData(), MTree.class, 30, "src/main/java/bp/storedAlgos/mtree/Mpeg");
    }

    /*------------------------------------------------M-index--------------------------------------------------------*/

    public static void createAndStoreMIndexSift() throws CapacityFullException, IOException, InstantiationException, AlgorithmMethodException {
        createAndStoreAlgorithm(new SiftData(), MIndexAlgorithm.class, 30, "src/main/java/bp/storedAlgos/mindex/Sift");
    }

    public static void createAndStoreMIndexRandom() throws CapacityFullException, IOException, InstantiationException, AlgorithmMethodException {
        createAndStoreAlgorithm(new RandomData(), MIndexAlgorithm.class, 30, "src/main/java/bp/storedAlgos/mindex/Random");
    }

    public static void createAndStoreMIndexDecaf() throws CapacityFullException, IOException, InstantiationException, AlgorithmMethodException {
        createAndStoreAlgorithm(new DecafData(), MIndexAlgorithm.class, 30, "src/main/java/bp/storedAlgos/mindex/Decaf");
    }

    public static void createAndStoreMIndexMpeg() throws CapacityFullException, IOException, InstantiationException, AlgorithmMethodException {
        createAndStoreAlgorithm(new MpegData(), MIndexAlgorithm.class, 30, "src/main/java/bp/storedAlgos/mindex/Mpeg");
    }

    public static <T extends LocalAbstractObject> void restoreAndExecuteQueries(DatasetData datasetData, int k,
                                                                                String algoFilePath,
                                                                                String filePathToStoreResults,
                                                                                boolean isApproxOp)
            throws IOException, ClassNotFoundException {
        SimilarityQueryEvaluator<? extends LocalAbstractObject> similarityQueryEvaluator = new SimilarityQueryEvaluator<>(
                Algorithm.restoreFromFile(algoFilePath), datasetData.queryFilePath, datasetData.queryCount,
                k, datasetData.dataFilePath, datasetData.dataObjectsCount, datasetData.objectClass);

        similarityQueryEvaluator.evaluateQueriesAndWriteResult(filePathToStoreResults, datasetData.groundTruthPath,
                datasetData.queryPattern, isApproxOp);
    }

    /*------------------------------------------------LAESA---------------------------------------------------------*/

    public static void restoreAndExecuteQueriesLaesaSift() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new SiftData(), 30, "src/main/java/bp/storedAlgos/laesa/Sift",
                "src/main/java/bp/results/laesa/LaesaSift.csv", false);
    }

    public static void restoreAndExecuteQueriesLaesaRandom() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new RandomData(), 30, "src/main/java/bp/storedAlgos/laesa/Random",
                "src/main/java/bp/results/laesa/LaesaRandom.csv", false);
    }

    public static void restoreAndExecuteQueriesLaesaDecaf() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new DecafData(), 30, "src/main/java/bp/storedAlgos/laesa/Decaf",
                "src/main/java/bp/results/laesa/LaesaDecaf.csv", false);
    }

    public static void restoreAndExecuteQueriesLaesaMpeg() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new MpegData(), 30, "src/main/java/bp/storedAlgos/laesa/Mpeg",
                "src/main/java/bp/results/laesa/LaesaMpeg.csv", false);
    }

    /*------------------------------------------------M-tree---------------------------------------------------------*/

    public static void restoreAndExecuteQueriesMTreeSift() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new SiftData(), 30, "src/main/java/bp/storedAlgos/mtree/Sift",
                "src/main/java/bp/results/mtree/MtreeSift.csv", false);
    }

    public static void restoreAndExecuteQueriesMTreeRandom() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new RandomData(), 30, "src/main/java/bp/storedAlgos/mtree/Random",
                "src/main/java/bp/results/mtree/MtreeRandom.csv", false);
    }

    public static void restoreAndExecuteQueriesMTreeDecaf() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new DecafData(), 30, "src/main/java/bp/storedAlgos/mtree/Decaf",
                "src/main/java/bp/results/mtree/MtreeDecaf.csv", false);
    }

    public static void restoreAndExecuteQueriesMTreeMpeg() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new MpegData(), 30, "src/main/java/bp/storedAlgos/mtree/Mpeg",
                "src/main/java/bp/results/mtree/MtreeMpeg.csv", false);
    }

    /*------------------------------------------------M-index---------------------------------------------------------*/

    public static void restoreAndExecuteQueriesMIndexSift() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new SiftData(), 30, "src/main/java/bp/storedAlgos/mindex/Sift",
                "src/main/java/bp/results/mindex/MIndexSift.csv", false);
    }

    public static void restoreAndExecuteQueriesMIndexRandom() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new RandomData(), 30, "src/main/java/bp/storedAlgos/mindex/Random",
                "src/main/java/bp/results/mindex/MIndexRandom.csv", false);
    }

    public static void restoreAndExecuteQueriesMIndexDecaf() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new DecafData(), 30, "src/main/java/bp/storedAlgos/mindex/Decaf",
                "src/main/java/bp/results/mindex/MIndexDecaf.csv", false);
    }

    public static void restoreAndExecuteQueriesMIndexMpeg() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new MpegData(), 30, "src/main/java/bp/storedAlgos/mindex/Mpeg",
                "src/main/java/bp/results/mindex/MIndexMpeg.csv", false);
    }

    /*--------------------------------------------Sequential-scan------------------------------------------------------*/

    public static void prepareAndExecuteSeqScan() throws IOException {
        SimilarityQueryEvaluator<ObjectFloatVectorL2> similarityQueryEvaluator = new SimilarityQueryEvaluator<>(
                new ParallelSequentialScan(4),
                "../D20_query_objects_uniform_distribution.data",
                10, 30, "../D20_data_objects_uniform_distribution.data",
                100000, ObjectFloatVectorL2.class);
        similarityQueryEvaluator.insertData();

        similarityQueryEvaluator.evaluateQueriesAndWriteResult("src/main/java/bp/results/SeqScan.csv",
                "add filepath", "add pattern", false);
    }
}
