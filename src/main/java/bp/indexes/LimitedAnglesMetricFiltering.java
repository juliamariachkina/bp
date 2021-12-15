package bp.indexes;

import bp.parsers.PivotCoefParser;
import bp.precomputedDistancesFilters.PrecomputedDistancesFixedArrayWithLimitedAnglesFilter;
import messif.algorithms.impl.SequentialScan;
import messif.buckets.CapacityFullException;
import messif.buckets.LocalBucket;
import messif.objects.LocalAbstractObject;
import messif.objects.util.AbstractObjectIterator;

import java.io.IOException;
import java.util.Map;

/**
 * This class implements the LAESA with an assumption of limited angles in triangles of distances algorithm
 * from the paper named "Data-Dependent Metric Filtering" by Vladimir Mic and Pavel Zezula.
 * It requires the precomputed pivot coefficients of all pivots, which will be used for filtering. Compute and store
 * the pivot coefficients using the PivotCoefs class. Then parse them using the PivotCoefParser class.
 */
public class LimitedAnglesMetricFiltering extends SequentialScan {
    /**
     * Parsed pivot coefficients required by this algorithm
     **/
    private final Map<String, Float> pivotCoefs;

    public LimitedAnglesMetricFiltering(LocalBucket bucket, AbstractObjectIterator<LocalAbstractObject> pivotIter,
                                        int pivotCount, boolean pivotDistsValidIfGiven, String pivotCoefsFilePath) throws IOException {
        super(bucket, pivotIter, pivotCount, pivotDistsValidIfGiven);
        pivotCoefs = new PivotCoefParser(pivotCoefsFilePath).parse();
    }

    public LimitedAnglesMetricFiltering(LocalBucket bucket, String pivotCoefsFilePath) throws IOException {
        super(bucket);
        pivotCoefs = new PivotCoefParser(pivotCoefsFilePath).parse();
    }

    public LimitedAnglesMetricFiltering(Class<? extends LocalBucket> bucketClass, Map<String, Object> bucketClassParams,
                                        AbstractObjectIterator<LocalAbstractObject> pivotIter, int pivotCount,
                                        boolean pivotDistsValidIfGiven, String pivotCoefsFilePath) throws CapacityFullException, InstantiationException, IOException {
        super(bucketClass, bucketClassParams, pivotIter, pivotCount, pivotDistsValidIfGiven);
        pivotCoefs = new PivotCoefParser(pivotCoefsFilePath).parse();
    }

    public LimitedAnglesMetricFiltering(Class<? extends LocalBucket> bucketClass, AbstractObjectIterator<LocalAbstractObject> pivotIter,
                                        int pivotCount, boolean pivotDistsValidIfGiven, String pivotCoefsFilePath) throws CapacityFullException, InstantiationException, IOException {
        super(bucketClass, pivotIter, pivotCount, pivotDistsValidIfGiven);
        pivotCoefs = new PivotCoefParser(pivotCoefsFilePath).parse();
    }

    public LimitedAnglesMetricFiltering(Class<? extends LocalBucket> bucketClass, Map<String, Object> bucketClassParams, String pivotCoefsFilePath)
            throws CapacityFullException, InstantiationException, IOException {
        super(bucketClass, bucketClassParams);
        pivotCoefs = new PivotCoefParser(pivotCoefsFilePath).parse();
    }

    public LimitedAnglesMetricFiltering(Class<? extends LocalBucket> bucketClass, String pivotCoefsFilePath) throws CapacityFullException, InstantiationException, IOException {
        super(bucketClass);
        pivotCoefs = new PivotCoefParser(pivotCoefsFilePath).parse();
    }

    public LimitedAnglesMetricFiltering(String pivotCoefsFilePath) throws CapacityFullException, InstantiationException, IOException {
        pivotCoefs = new PivotCoefParser(pivotCoefsFilePath).parse();
    }

    /**
     * Adds tightened precomputed distances between an object and the set of pivots to the
     * PrecomputedDistancesFixedArrayWithLimitedAnglesFilter.
     *
     * @param object object to which the distances from the pivots are evaluated
     */
    protected void addPrecompDist(LocalAbstractObject object) {
        PrecomputedDistancesFixedArrayWithLimitedAnglesFilter precompDist = object.getDistanceFilter(PrecomputedDistancesFixedArrayWithLimitedAnglesFilter.class);
        if (precompDist == null || !pivotDistsValidIfGiven) {
            if (precompDist == null)
                precompDist = new PrecomputedDistancesFixedArrayWithLimitedAnglesFilter(object, pivotCoefs);
            precompDist.addPrecompDist(pivots, object);
        }
    }
}
