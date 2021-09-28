package bp;

import bp.datasets.*;
import bp.evaluators.SimilarityQueryEvaluator;
import bp.utils.Utility;
import messif.algorithms.Algorithm;
import messif.algorithms.AlgorithmMethodException;
import messif.algorithms.impl.ParallelSequentialScan;
import messif.algorithms.impl.SequentialScan;
import messif.buckets.CapacityFullException;
import messif.objects.LocalAbstractObject;
import messif.objects.impl.ObjectFloatVectorL2;
import messif.objects.util.AbstractObjectIterator;
import mtree.MTree;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    private static final Logger LOG = Logger.getLogger(Main.class.getName());

    public static void main(String args[])
            throws IOException, CapacityFullException, InstantiationException, ClassNotFoundException, AlgorithmMethodException {

        System.setErr(new PrintStream(new FileOutputStream("src/main/java/bp/errorOutputs/laesa/Decaf.txt")));
        restoreAndExecuteQueriesLaesaDecaf();
//        System.setErr(new PrintStream(new FileOutputStream("src/main/java/bp/errorOutputs/laesa/Mpeg.txt")));
//        restoreAndExecuteQueriesLaesaMpeg();
//        System.setErr(new PrintStream(new FileOutputStream("src/main/java/bp/errorOutputs/laesa/Sift.txt")));
//        restoreAndExecuteQueriesLaesaSift();
//        System.setErr(new PrintStream(new FileOutputStream("src/main/java/bp/errorOutputs/laesa/Random.txt")));
//        restoreAndExecuteQueriesLaesaRandom();
//
//        createAndStoreMTreeRandom();
//        createAndStoreMTreeSift();
//        createAndStoreMTreeMpeg();
//        createAndStoreMTreeDecaf();
//        System.setErr(new PrintStream(new FileOutputStream("src/main/java/bp/errorOutputs/mtree/Random.txt")));
//        restoreAndExecuteQueriesMTreeRandom();
//        System.setErr(new PrintStream(new FileOutputStream("src/main/java/bp/errorOutputs/mtree/Sift.txt")));
//        restoreAndExecuteQueriesMTreeSift();
//        System.setErr(new PrintStream(new FileOutputStream("src/main/java/bp/errorOutputs/mtree/Mpeg.txt")));
//        restoreAndExecuteQueriesMTreeMpeg();
//        System.setErr(new PrintStream(new FileOutputStream("src/main/java/bp/errorOutputs/mtree/Decaf.txt")));
//        restoreAndExecuteQueriesMTreeDecaf();
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
            int internalNodeCapacity = 50 * pivotIter.getCurrentObject().getSize();
            algorithm = new MTree(internalNodeCapacity, internalNodeCapacity * 4L, datasetData.pivotCount,
                    pivotIter, datasetData.pivotCount, datasetData.pivotCount);
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

    public static <T extends LocalAbstractObject> void restoreAndExecuteQueries(DatasetData datasetData, int k,
                                                                                String algoFilePath,
                                                                                String filePathToStoreResults)
            throws IOException, ClassNotFoundException {
        SimilarityQueryEvaluator<? extends LocalAbstractObject> similarityQueryEvaluator = new SimilarityQueryEvaluator<>(
                Algorithm.restoreFromFile(algoFilePath), datasetData.queryFilePath, datasetData.queryCount,
                k, datasetData.dataFilePath, datasetData.dataObjectsCount, datasetData.objectClass);

        similarityQueryEvaluator.evaluateQueriesAndWriteResult(filePathToStoreResults, datasetData.groundTruthPath,
                datasetData.queryPattern);
    }

    /*------------------------------------------------LAESA---------------------------------------------------------*/

    public static void restoreAndExecuteQueriesLaesaSift() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new SiftData(), 30, "src/main/java/bp/storedAlgos/laesa/Sift",
                "src/main/java/bp/results/laesa/LaesaSift.csv");
    }

    public static void restoreAndExecuteQueriesLaesaRandom() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new RandomData(), 30, "src/main/java/bp/storedAlgos/laesa/Random",
                "src/main/java/bp/results/laesa/LaesaRandom.csv");
    }

    public static void restoreAndExecuteQueriesLaesaDecaf() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new DecafData(), 30, "src/main/java/bp/storedAlgos/laesa/Decaf",
                "src/main/java/bp/results/laesa/LaesaDecaf.csv");
    }

    public static void restoreAndExecuteQueriesLaesaMpeg() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new MpegData(), 30, "src/main/java/bp/storedAlgos/laesa/Mpeg",
                "src/main/java/bp/results/laesa/LaesaMpeg.csv");
    }

    /*------------------------------------------------M-tree---------------------------------------------------------*/

    public static void restoreAndExecuteQueriesMTreeSift() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new SiftData(), 30, "src/main/java/bp/storedAlgos/mtree/Sift",
                "src/main/java/bp/results/mtree/MtreeSift.csv");
    }

    public static void restoreAndExecuteQueriesMTreeRandom() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new RandomData(), 30, "src/main/java/bp/storedAlgos/mtree/Random",
                "src/main/java/bp/results/mtree/MtreeRandom.csv");
    }

    public static void restoreAndExecuteQueriesMTreeDecaf() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new DecafData(), 30, "src/main/java/bp/storedAlgos/mtree/Decaf",
                "src/main/java/bp/results/mtree/MtreeDecaf.csv");
    }

    public static void restoreAndExecuteQueriesMTreeMpeg() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new MpegData(), 30, "src/main/java/bp/storedAlgos/mtree/Mpeg",
                "src/main/java/bp/results/mtree/MtreeMpeg.csv");
    }

    public static void prepareAndExecuteSeqScan() throws IOException {
        SimilarityQueryEvaluator<ObjectFloatVectorL2> similarityQueryEvaluator = new SimilarityQueryEvaluator<>(
                new ParallelSequentialScan(4),
                "../D20_query_objects_uniform_distribution.data",
                10, 30, "../D20_data_objects_uniform_distribution.data",
                100000, ObjectFloatVectorL2.class);
        similarityQueryEvaluator.insertData();

        similarityQueryEvaluator.evaluateQueriesAndWriteResult("src/main/java/bp/results/SeqScan.csv",
                "add filepath", "add pattern");
    }
}
