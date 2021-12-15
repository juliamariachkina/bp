package bp;

import bp.datasets.*;
import bp.evaluators.SimilarityQueryEvaluator;
import bp.indexes.LimitedAnglesMetricFiltering;
import bp.utils.Utility;
import messif.algorithms.Algorithm;
import messif.algorithms.AlgorithmMethodException;
import messif.algorithms.impl.SequentialScan;
import messif.buckets.CapacityFullException;
import messif.objects.LocalAbstractObject;
import messif.objects.util.AbstractObjectIterator;
import mindex.algorithms.MIndexAlgorithm;
import mtree.MTree;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    private static final Logger LOG = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
    }

    /**
     * Creates a new instance of a filtering technique (algorithm), inserts all data objects from the dataset
     * and stores it to the algorithm to the file at the filePathToStoreAlgo.
     *
     * @param datasetData         metadata of a specific dataset
     * @param algorithmClass      class of an algorithm to initialise
     * @param k                   parameter of the kNN queries
     * @param filePathToStoreAlgo filepath where to store the algorithm
     * @throws CapacityFullException    propagates the exception
     * @throws InstantiationException   propagates the exception
     * @throws IOException              propagates the exception
     * @throws AlgorithmMethodException propagates the exception
     */
    public static void createAndStoreAlgorithm(DatasetData datasetData, Class<? extends Algorithm> algorithmClass,
                                               int k, String filePathToStoreAlgo)
            throws CapacityFullException, InstantiationException, IOException, AlgorithmMethodException {
        LOG.log(Level.INFO, "Create and Store algorithm method starts");
        AbstractObjectIterator<LocalAbstractObject> pivotIter = Utility.getObjectsIterator(datasetData.pivotFilePath, datasetData.objectClass);
        LOG.log(Level.INFO, "Pivot iterator created");

        Algorithm algorithm = createAlgorithm(datasetData, algorithmClass, pivotIter);
        LOG.log(Level.INFO, "Algorithm initialised");

        SimilarityQueryEvaluator<? extends LocalAbstractObject> similarityQueryEvaluator =
                new SimilarityQueryEvaluator<>(algorithm, k, datasetData);
        similarityQueryEvaluator.insertData();
        LOG.log(Level.INFO, "Data objects inserted");

        similarityQueryEvaluator.storeToFile(filePathToStoreAlgo);
        LOG.log(Level.INFO, "Algorithm stored to a file");
    }

    /**
     * Creates an instance of an algorithm according to the parameters.
     *
     * @param datasetData    metadata of a specific dataset
     * @param algorithmClass class of an algorithm to initialise
     * @param pivotIter      iterator of dataset pivots (used by the algorithm)
     * @return a new algorithm instance
     * @throws CapacityFullException    propagates the exception
     * @throws InstantiationException   propagates the exception
     * @throws AlgorithmMethodException propagates the exception
     * @throws IOException              propagates the exception
     */
    private static Algorithm createAlgorithm(DatasetData datasetData, Class<? extends Algorithm> algorithmClass,
                                             AbstractObjectIterator<LocalAbstractObject> pivotIter)
            throws CapacityFullException, InstantiationException, AlgorithmMethodException, IOException {
        if (algorithmClass.equals(SequentialScan.class))
            return new SequentialScan(datasetData.bucketClass, pivotIter, datasetData.pivotCount, true);
        if (algorithmClass.equals(MIndexAlgorithm.class))
            return createMIndex(datasetData);
        if (algorithmClass.equals(LimitedAnglesMetricFiltering.class))
            return new LimitedAnglesMetricFiltering(datasetData.bucketClass, pivotIter, datasetData.pivotCount,
                    true, datasetData.pivotCoefsFilePath);
        return createMtree(datasetData, pivotIter);
    }

    private static MTree createMtree(DatasetData datasetData, AbstractObjectIterator<LocalAbstractObject> pivotIter)
            throws AlgorithmMethodException, InstantiationException {
        int internalNodeCapacity = 50 * Utility.getObjectsList(datasetData.pivotFilePath, datasetData.objectClass, 1)
                .get(0).getSize();
        return new MTree(internalNodeCapacity, internalNodeCapacity * 4L, datasetData.pivotCount, pivotIter,
                datasetData.pivotCount, datasetData.pivotCount);
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

    // Below are the methods that I used in my experiments to create the algorithms

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

    /*-----------------------------------------LAESA with limited angles---------------------------------------------*/

    public static void createAndStoreLaesaWithLimitedAnglesSift()
            throws CapacityFullException, IOException, InstantiationException, AlgorithmMethodException {
        createAndStoreAlgorithm(new SiftData(), LimitedAnglesMetricFiltering.class, 30,
                "src/main/java/bp/storedAlgos/laesaLimAngles/Sift");
    }

    public static void createAndStoreLaesaWithLimitedAnglesRandom()
            throws CapacityFullException, IOException, InstantiationException, AlgorithmMethodException {
        createAndStoreAlgorithm(new RandomData(), LimitedAnglesMetricFiltering.class, 30,
                "src/main/java/bp/storedAlgos/laesaLimAngles/Random");
    }

    public static void createAndStoreLaesaWithLimitedAnglesDecaf()
            throws CapacityFullException, IOException, InstantiationException, AlgorithmMethodException {
        createAndStoreAlgorithm(new DecafData(), LimitedAnglesMetricFiltering.class, 30,
                "src/main/java/bp/storedAlgos/laesaLimAngles/Decaf");
    }

    public static void createAndStoreLaesaWithLimitedAnglesMpeg()
            throws CapacityFullException, IOException, InstantiationException, AlgorithmMethodException {
        createAndStoreAlgorithm(new MpegData(), LimitedAnglesMetricFiltering.class, 30,
                "src/main/java/bp/storedAlgos/laesaLimAngles/Mpeg");
    }
    /*--------------------------------------------------------------------------------------------------------------*/

    /**
     * Restores an algorithm form the file, evaluates all queries, writes the results to the file
     * at the filePathToStoreResults.
     *
     * @param datasetData            metadata of a specific dataset
     * @param k                      parameter of the kNN queries
     * @param algoFilePath           filepath to the file where the algorithm is stored
     * @param filePathToStoreResults filepath to the file where to store the results
     * @param isApproxOp             defines what type of queries to evaluate: precise or approximate
     * @throws IOException            propagates the exception
     * @throws ClassNotFoundException propagates the exception
     */
    public static void restoreAndExecuteQueries(DatasetData datasetData, int k, String algoFilePath,
                                                String filePathToStoreResults, boolean isApproxOp)
            throws IOException, ClassNotFoundException {
        SimilarityQueryEvaluator<? extends LocalAbstractObject> similarityQueryEvaluator =
                new SimilarityQueryEvaluator<>(Algorithm.restoreFromFile(algoFilePath), k, datasetData);

        similarityQueryEvaluator.evaluateQueriesAndWriteResult(filePathToStoreResults, datasetData.groundTruthPath,
                datasetData.queryPattern, isApproxOp);
    }

    // Below are the methods that I used in my experiments to evaluate the queries

    /*------------------------------------------------LAESA---------------------------------------------------------*/

    /**
     * Restores and executes queries using the LAESA on the SIFT dtaaset. By redirecting the error output stream
     * to a file we can store the error output produced during query processing. This is exactly how we get the
     * candidate sets from query evaluations. We added a line of code to all distance functions that we use that writes
     * to the error output the URIs of objects between which it computes the distance.
     *
     * @throws IOException            propagates the exception
     * @throws ClassNotFoundException propagates the exception
     */
    public static void restoreAndExecuteQueriesLaesaSift() throws IOException, ClassNotFoundException {
        OutputStream stream = Utility.getOutputStream(("src/main/java/bp/errorOutputs/laesa/Sift.txt.gz"));
        System.setErr(new PrintStream(stream));
        restoreAndExecuteQueries(new SiftData(), 30, "src/main/java/bp/storedAlgos/laesa/Sift",
                "src/main/java/bp/results/laesa/LaesaSift.csv", false);
        stream.flush();
        stream.close();
    }

    public static void restoreAndExecuteQueriesLaesaRandom() throws IOException, ClassNotFoundException {
        OutputStream stream = Utility.getOutputStream(("src/main/java/bp/errorOutputs/laesa/Random.txt.gz"));
        System.setErr(new PrintStream(stream));
        restoreAndExecuteQueries(new RandomData(), 30, "src/main/java/bp/storedAlgos/laesa/Random",
                "src/main/java/bp/results/laesa/LaesaRandom.csv", false);
        stream.flush();
        stream.close();
    }

    public static void restoreAndExecuteQueriesLaesaDecaf() throws IOException, ClassNotFoundException {
        OutputStream stream = Utility.getOutputStream(("src/main/java/bp/errorOutputs/laesa/Decaf.txt.gz"));
        System.setErr(new PrintStream(stream));
        restoreAndExecuteQueries(new DecafData(), 30, "src/main/java/bp/storedAlgos/laesa/Decaf",
                "src/main/java/bp/results/laesa/LaesaDecaf.csv", false);
        stream.flush();
        stream.close();
    }

    public static void restoreAndExecuteQueriesLaesaMpeg() throws IOException, ClassNotFoundException {
        OutputStream stream = Utility.getOutputStream(("src/main/java/bp/errorOutputs/laesa/Mpeg.txt.gz"));
        System.setErr(new PrintStream(stream));
        restoreAndExecuteQueries(new MpegData(), 30, "src/main/java/bp/storedAlgos/laesa/Mpeg",
                "src/main/java/bp/results/laesa/LaesaMpeg.csv", false);
        stream.flush();
        stream.close();
    }

    /*------------------------------------------------M-tree---------------------------------------------------------*/

    public static void restoreAndExecuteQueriesMTreeSift() throws IOException, ClassNotFoundException {
        OutputStream stream = Utility.getOutputStream("src/main/java/bp/errorOutputs/mtree/Sift.txt.gz");
        System.setErr(new PrintStream(stream));
        restoreAndExecuteQueries(new SiftData(), 30, "src/main/java/bp/storedAlgos/mtree/Sift",
                "src/main/java/bp/results/mtree/MtreeSift.csv", false);
        stream.flush();
        stream.close();
    }

    public static void restoreAndExecuteQueriesMTreeRandom() throws IOException, ClassNotFoundException {
        OutputStream stream = Utility.getOutputStream("src/main/java/bp/errorOutputs/mtree/Random.txt.gz");
        System.setErr(new PrintStream(stream));
        restoreAndExecuteQueries(new RandomData(), 30, "src/main/java/bp/storedAlgos/mtree/Random",
                "src/main/java/bp/results/mtree/MtreeRandom.csv", false);
        stream.flush();
        stream.close();
    }

    public static void restoreAndExecuteQueriesMTreeDecaf() throws IOException, ClassNotFoundException {
        OutputStream stream = Utility.getOutputStream("src/main/java/bp/errorOutputs/mtree/Decaf.txt.gz");
        System.setErr(new PrintStream(stream));
        restoreAndExecuteQueries(new DecafData(), 30, "src/main/java/bp/storedAlgos/mtree/Decaf",
                "src/main/java/bp/results/mtree/MtreeDecaf.csv", false);
        stream.flush();
        stream.close();
    }

    public static void restoreAndExecuteQueriesMTreeMpeg() throws IOException, ClassNotFoundException {
        OutputStream stream = Utility.getOutputStream("src/main/java/bp/errorOutputs/mtree/Mpeg.txt.gz");
        System.setErr(new PrintStream(stream));
        restoreAndExecuteQueries(new MpegData(), 30, "src/main/java/bp/storedAlgos/mtree/Mpeg",
                "src/main/java/bp/results/mtree/MtreeMpeg.csv", false);
        stream.flush();
        stream.close();
    }

    /*------------------------------------------------M-index---------------------------------------------------------*/

    public static void restoreAndExecuteQueriesMIndexSift() throws IOException, ClassNotFoundException {
        OutputStream stream = Utility.getOutputStream("src/main/java/bp/errorOutputs/mindex/Sift.txt.gz");
        System.setErr(new PrintStream(stream));
        restoreAndExecuteQueries(new SiftData(), 30, "src/main/java/bp/storedAlgos/mindex/Sift",
                "src/main/java/bp/results/mindex/MIndexSift.csv", false);
        stream.flush();
        stream.close();
    }

    public static void restoreAndExecuteQueriesMIndexRandom() throws IOException, ClassNotFoundException {
        OutputStream stream = Utility.getOutputStream("src/main/java/bp/errorOutputs/mindex/Random.txt.gz");
        System.setErr(new PrintStream(stream));
        restoreAndExecuteQueries(new RandomData(), 30, "src/main/java/bp/storedAlgos/mindex/Random",
                "src/main/java/bp/results/mindex/MIndexRandom.csv", false);
        stream.flush();
        stream.close();
    }

    public static void restoreAndExecuteQueriesMIndexDecaf() throws IOException, ClassNotFoundException {
        OutputStream stream = Utility.getOutputStream("src/main/java/bp/errorOutputs/mindex/Decaf.txt.gz");
        System.setErr(new PrintStream(stream));
        restoreAndExecuteQueries(new DecafData(), 30, "src/main/java/bp/storedAlgos/mindex/Decaf",
                "src/main/java/bp/results/mindex/MIndexDecaf.csv", false);
        stream.flush();
        stream.close();
    }

    public static void restoreAndExecuteQueriesMIndexMpeg() throws IOException, ClassNotFoundException {
        OutputStream stream = Utility.getOutputStream("src/main/java/bp/errorOutputs/mindex/Mpeg.txt.gz");
        System.setErr(new PrintStream(stream));
        restoreAndExecuteQueries(new MpegData(), 30, "src/main/java/bp/storedAlgos/mindex/Mpeg",
                "src/main/java/bp/results/mindex/MIndexMpeg.csv", false);
        stream.flush();
        stream.close();
    }

    /*-----------------------------------LAESA with limited angles--------------------------------------------*/

    public static void restoreAndExecuteQueriesLaesaWithLimitedAnglesSift() throws IOException, ClassNotFoundException {
        OutputStream stream = Utility.getOutputStream("src/main/java/bp/errorOutputs/laesaLimAngles/Sift.txt.gz");
        System.setErr(new PrintStream(stream));
        restoreAndExecuteQueries(new SiftData(), 30, "src/main/java/bp/storedAlgos/laesaLimAngles/Sift",
                "src/main/java/bp/results/laesaLimAngles/LaesaLimAnglesSift.csv", true);
        stream.flush();
        stream.close();
    }

    public static void restoreAndExecuteQueriesLaesaWithLimitedAnglesRandom() throws IOException, ClassNotFoundException {
        OutputStream stream = Utility.getOutputStream("src/main/java/bp/errorOutputs/laesaLimAngles/Random.txt.gz");
        System.setErr(new PrintStream(stream));
        restoreAndExecuteQueries(new RandomData(), 30, "src/main/java/bp/storedAlgos/laesaLimAngles/Random",
                "src/main/java/bp/results/laesaLimAngles/LaesaLimAnglesRandom.csv", true);
        stream.flush();
        stream.close();
    }

    public static void restoreAndExecuteQueriesLaesaWithLimitedAnglesDecaf() throws IOException, ClassNotFoundException {
        OutputStream stream = Utility.getOutputStream("src/main/java/bp/errorOutputs/laesaLimAngles/Decaf.txt.gz");
        System.setErr(new PrintStream(stream));
        restoreAndExecuteQueries(new DecafData(), 30, "src/main/java/bp/storedAlgos/laesaLimAngles/Decaf",
                "src/main/java/bp/results/laesaLimAngles/LaesaLimAnglesDecaf.csv", true);
        stream.flush();
        stream.close();
    }

    public static void restoreAndExecuteQueriesLaesaWithLimitedAnglesMpeg() throws IOException, ClassNotFoundException {
        OutputStream stream = Utility.getOutputStream("src/main/java/bp/errorOutputs/laesaLimAngles/Mpeg.txt.gz");
        System.setErr(new PrintStream(stream));
        restoreAndExecuteQueries(new MpegData(), 30, "src/main/java/bp/storedAlgos/laesaLimAngles/Mpeg",
                "src/main/java/bp/results/laesaLimAngles/LaesaLimAnglesMpeg.csv", true);
        stream.flush();
        stream.close();
    }
}
