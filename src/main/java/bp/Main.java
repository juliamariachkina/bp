package bp;

import bp.datasets.*;
import bp.evaluators.SimilarityQueryEvaluator;
import bp.evaluators.SynergyEffectivenessEvaluator;
import bp.indexes.LimitedAnglesMetricFiltering;
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
import messif.operations.data.InsertOperation;
import mindex.algorithms.MIndexAlgorithm;
import mtree.MTree;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Array;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    private static final Logger LOG = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws IOException, CapacityFullException, AlgorithmMethodException, InstantiationException, ClassNotFoundException {
        restoreAndExecuteQueriesLaesaWithLimitedAnglesRandom();

//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/random/GHP_50_128.txt.gz", new RandomData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/random/GHP_50_128.data.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/random/GHP_50_192.txt.gz", new RandomData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/random/GHP_50_192.data.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/random/GHP_50_256.txt.gz", new RandomData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/random/GHP_50_256.data.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/random/GHP_50_64.txt.gz", new RandomData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/random/GHP_50_64.data.gz");
//
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/random/GHP_80_128.txt.gz", new RandomData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/random/GHP_80_128.data.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/random/GHP_80_192.txt.gz", new RandomData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/random/GHP_80_192.data.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/random/GHP_80_256.txt.gz", new RandomData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/random/GHP_80_256.data.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/random/GHP_80_64.txt.gz", new RandomData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/random/GHP_80_64.data.gz");
//
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/sift/GHP_50_128.txt.gz", new SiftData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/sift/GHP_50_128.data.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/sift/GHP_50_192.txt.gz", new SiftData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/sift/GHP_50_192.data.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/sift/GHP_50_256.txt.gz", new SiftData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/sift/GHP_50_256.data.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/sift/GHP_50_64.txt.gz", new SiftData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/sift/GHP_50_64.data.gz");
//
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/sift/GHP_80_128.txt.gz", new SiftData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/sift/GHP_80_128.data.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/sift/GHP_80_192.txt.gz", new SiftData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/sift/GHP_80_192.data.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/sift/GHP_80_256.txt.gz", new SiftData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/sift/GHP_80_256.data.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/sift/GHP_80_64.txt.gz", new SiftData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/sift/GHP_80_64.data.gz");
//
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/mpeg/GHP_50_128.txt.gz", new MpegData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/mpeg7/GHP_50_128.data.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/mpeg/GHP_50_192.txt.gz", new MpegData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/mpeg7/GHP_50_192.data.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/mpeg/GHP_50_256.txt.gz", new MpegData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/mpeg7/GHP_50_256.data.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/mpeg/GHP_50_64.txt.gz", new MpegData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/mpeg7/GHP_50_64.data.gz");
//
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/mpeg/GHP_80_128.txt.gz", new MpegData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/mpeg7/GHP_80_128.data.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/mpeg/GHP_80_192.txt.gz", new MpegData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/mpeg7/GHP_80_192.data.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/mpeg/GHP_80_256.txt.gz", new MpegData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/mpeg7/GHP_80_256.data.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/mpeg/GHP_80_64.txt.gz", new MpegData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/mpeg7/GHP_80_64.data.gz");
//
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/decaf/GHP_50_128.txt.gz", new DecafData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/decaf/GHP_50_128.data.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/decaf/GHP_50_192.txt.gz", new DecafData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/decaf/GHP_50_192.data.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/decaf/GHP_50_256.txt.gz", new DecafData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/decaf/GHP_50_256.data.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/decaf/GHP_50_64.txt.gz", new DecafData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/decaf/GHP_50_64.data.gz");
//
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/decaf/GHP_80_128.txt.gz", new DecafData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/decaf/GHP_80_128.data.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/decaf/GHP_80_192.txt.gz", new DecafData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/decaf/GHP_80_192.data.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/decaf/GHP_80_256.txt.gz", new DecafData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/decaf/GHP_80_256.data.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/sketches/decaf/GHP_80_64.txt.gz", new DecafData())
//                .reduceErrOutputFilesToMedianDistComp("/home/xmic/2021_BP_Iluliia/err_output_knnSearchOnSketches/decaf/GHP_80_64.data.gz");


//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/laesa/Random.txt.gz", new RandomData())
//                .reduceErrOutputFilesToMedianDistComp("src/main/java/bp/errorOutputs/laesa/Random.txt.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/mtree/Random.txt.gz", new RandomData())
//                .reduceErrOutputFilesToMedianDistComp("src/main/java/bp/errorOutputs/mtree/Random.txt.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/mindex/Random.txt.gz", new RandomData())
//                .reduceErrOutputFilesToMedianDistComp("src/main/java/bp/errorOutputs/mindex/Random.txt.gz");

//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/laesa/Sift.txt.gz", new SiftData())
//                .reduceErrOutputFilesToMedianDistComp("src/main/java/bp/errorOutputs/laesa/Sift.txt.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/laesa/Mpeg.txt.gz", new MpegData())
//                .reduceErrOutputFilesToMedianDistComp("src/main/java/bp/errorOutputs/laesa/Mpeg.txt.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/laesa/Decaf.txt.gz", new DecafData())
//                .reduceErrOutputFilesToMedianDistComp("src/main/java/bp/errorOutputs/laesa/Decaf.txt.gz");
//
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/mtree/Sift.txt.gz", new SiftData())
//                .reduceErrOutputFilesToMedianDistComp("src/main/java/bp/errorOutputs/mtree/Sift.txt.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/mtree/Mpeg.txt.gz", new MpegData())
//                .reduceErrOutputFilesToMedianDistComp("src/main/java/bp/errorOutputs/mtree/Mpeg.txt.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/mtree/Decaf.txt.gz", new DecafData())
//                .reduceErrOutputFilesToMedianDistComp("src/main/java/bp/errorOutputs/mtree/Decaf.txt.gz");
//
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/mindex/Sift.txt.gz", new SiftData())
//                .reduceErrOutputFilesToMedianDistComp("src/main/java/bp/errorOutputs/mindex/Sift.txt.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/mindex/Mpeg.txt.gz", new MpegData())
//                .reduceErrOutputFilesToMedianDistComp("src/main/java/bp/errorOutputs/mindex/Mpeg.txt.gz");
//        new SynergyEffectivenessEvaluator("src/main/java/bp/reducedOutput/mindex/Decaf.txt.gz", new DecafData())
//                .reduceErrOutputFilesToMedianDistComp("src/main/java/bp/errorOutputs/mindex/Decaf.txt.gz");
    }

    public static void createAndStoreAlgorithm(DatasetData datasetData, Class<? extends Algorithm> algorithmClass,
                                               int k, String filePathToStoreAlgo)
            throws CapacityFullException, InstantiationException, IOException, AlgorithmMethodException {
        LOG.log(Level.INFO, "Create and Store algorithm method starts");
        AbstractObjectIterator<LocalAbstractObject> pivotIter = Utility.getObjectsIterator(datasetData.pivotFilePath, datasetData.objectClass);
        LOG.log(Level.INFO, "Pivot iterator created");

        Algorithm algorithm = createAlgorithm(datasetData, algorithmClass, pivotIter);
        LOG.log(Level.INFO, "Algorithm initialised");

        SimilarityQueryEvaluator<? extends LocalAbstractObject> similarityQueryEvaluator = new SimilarityQueryEvaluator<>(
                algorithm, datasetData.queryFilePath, datasetData.queryCount, k,
                datasetData.dataFilePath, datasetData.dataObjectsCount, datasetData.objectClass);
        similarityQueryEvaluator.insertData();
        LOG.log(Level.INFO, "Data objects inserted");

        similarityQueryEvaluator.storeToFile(filePathToStoreAlgo);
        LOG.log(Level.INFO, "Algorithm stored to a file");
    }

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

    /*-----------------------------------LAESA with limited angles--------------------------------------------*/

    public static void restoreAndExecuteQueriesLaesaWithLimitedAnglesSift() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new SiftData(), 30, "src/main/java/bp/storedAlgos/laesaLimAngles/Sift",
                "src/main/java/bp/results/laesaLimAngles/LaesaLimAnglesSift.csv", true);
    }

    public static void restoreAndExecuteQueriesLaesaWithLimitedAnglesRandom() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new RandomData(), 30, "src/main/java/bp/storedAlgos/laesaLimAngles/Random",
                "src/main/java/bp/results/laesaLimAngles/LaesaLimAnglesRandom.csv", true);
    }

    public static void restoreAndExecuteQueriesLaesaWithLimitedAnglesDecaf() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new DecafData(), 30, "src/main/java/bp/storedAlgos/laesaLimAngles/Decaf",
                "src/main/java/bp/results/laesaLimAngles/LaesaLimAnglesDecaf.csv", true);
    }

    public static void restoreAndExecuteQueriesLaesaWithLimitedAnglesMpeg() throws IOException, ClassNotFoundException {
        restoreAndExecuteQueries(new MpegData(), 30, "src/main/java/bp/storedAlgos/laesaLimAngles/Mpeg",
                "src/main/java/bp/results/laesaLimAngles/LaesaLimAnglesMpeg.csv", true);
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
