package bp;

import bp.datasets.*;
import bp.evaluators.SimilarityQueryEvaluator;
import bp.utils.Utility;
import messif.algorithms.Algorithm;
import messif.algorithms.impl.ParallelSequentialScan;
import messif.algorithms.impl.SequentialScan;
import messif.buckets.CapacityFullException;
import messif.objects.LocalAbstractObject;
import messif.objects.impl.ObjectFloatVectorL2;
import messif.objects.util.AbstractObjectIterator;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    private static final Logger LOG = Logger.getLogger(Main.class.getName());

    public static void main(String args[])
            throws IOException, CapacityFullException, InstantiationException, ClassNotFoundException {
        System.setErr(new PrintStream(new FileOutputStream("src/main/java/bp/errorOutputs/laesaMpeg.txt")));
//      createAndStoreLaesaSift();
//	    restoreAndExecuteQueriesLaesaSift();
        createAndStoreLaesaMpeg();
        restoreAndExecuteQueriesLaesaMpeg();
        //restoreAndExecuteQueriesLaesaDecaf();
    }

    public static void createAndStoreAlgorithm(DatasetData datasetData, int k, String filePathToStoreAlgo)
            throws CapacityFullException, InstantiationException, IOException {
        LOG.log(Level.INFO, "Create and Store algorithm method starts");
        AbstractObjectIterator<LocalAbstractObject> pivotIter = Utility.getObjectsIterator(datasetData.pivotFilePath, datasetData.objectClass);
        LOG.log(Level.INFO, "Pivot iterator created");
        Algorithm aLgorithm = new SequentialScan(datasetData.bucketClass, pivotIter, datasetData.pivotCount, true);
        LOG.log(Level.INFO, "Algorithm initialised");

        SimilarityQueryEvaluator<? extends LocalAbstractObject> similarityQueryEvaluator = new SimilarityQueryEvaluator<>(
                aLgorithm, datasetData.queryFilePath, datasetData.queryCount, k,
                datasetData.dataFilePath, datasetData.dataObjectsCount, datasetData.objectClass);
        similarityQueryEvaluator.insertData();
        LOG.log(Level.INFO, "Data objects inserted");

        similarityQueryEvaluator.storeToFile(filePathToStoreAlgo);
        LOG.log(Level.INFO, "Algorithm stored to a file");
    }

    public static void createAndStoreLaesaSift() throws CapacityFullException, IOException, InstantiationException {
        createAndStoreAlgorithm(new SiftData(), 30, "src/main/java/bp/storedAlgos/laesaSift");
    }

    public static void createAndStoreLaesaRandom() throws CapacityFullException, IOException, InstantiationException {
        createAndStoreAlgorithm(new RandomData(), 30, "src/main/java/bp/storedAlgos/laesaRandom");
    }

    public static void createAndStoreLaesaDecaf() throws CapacityFullException, IOException, InstantiationException {
        createAndStoreAlgorithm(new DecafData(), 30, "src/main/java/bp/storedAlgos/laesaDecaf");
    }

    public static void createAndStoreLaesaMpeg() throws CapacityFullException, IOException, InstantiationException {
        createAndStoreAlgorithm(new MpegData(), 30, "src/main/java/bp/storedAlgos/laesaMpeg");
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

    public static void restoreAndExecuteQueriesLaesaSift() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new SiftData(), 30, "src/main/java/bp/storedAlgos/laesaSift",
                "src/main/java/bp/results/LaesaSiftWithGroundTruth.csv");
    }

    public static void restoreAndExecuteQueriesLaesaRandom() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new RandomData(), 30, "src/main/java/bp/storedAlgos/laesaRandom",
                "src/main/java/bp/results/LaesaRandom.csv");
    }

    public static void restoreAndExecuteQueriesLaesaDecaf() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new DecafData(), 30, "src/main/java/bp/storedAlgos/laesaDecaf",
                "src/main/java/bp/results/LaesaDecafWithGroundTruth.csv");
    }

    public static void restoreAndExecuteQueriesLaesaMpeg() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new MpegData(), 30, "src/main/java/bp/storedAlgos/laesaMpeg",
                "src/main/java/bp/results/LaesaMpegWithGroundTruth.csv");
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
