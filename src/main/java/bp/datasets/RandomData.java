package bp.datasets;

import messif.buckets.impl.MemoryStorageBucket;
import messif.objects.impl.ObjectFloatVectorL2;

/**
 * This class stores metadata of the RANDOM20 dataset.
 */
public class RandomData extends DatasetData {

    /**
     * Creates a new instance of RandomData by passing all the required parameters to its ancestor DatasetData.
     */
    public RandomData() {
        super(ObjectFloatVectorL2.class, MemoryStorageBucket.class, "../random/pivots_256", 512,
                "../random/query_1000", 1000, "../random/data_1M", 1000000,
                "../random/groundtruth_1000",
                "INFO: Algorithm processed: RangeQueryOperation <ObjectFloatVectorL2 \\((\\w\\d+)\\), .*",
                "src/main/java/bp/computedPivotCoefs/Random.csv");
    }
}
