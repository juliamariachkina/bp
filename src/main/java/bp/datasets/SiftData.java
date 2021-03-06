package bp.datasets;

import messif.buckets.impl.MemoryStorageBucket;
import messif.objects.impl.ObjectFloatVectorL2;

/**
 * This class stores metadata of the SIFT dataset.
 */
public class SiftData extends DatasetData {

    /**
     * Creates a new instance of SiftData by passing all the required parameters to its ancestor DatasetData.
     */
    public SiftData() {
        super(ObjectFloatVectorL2.class, MemoryStorageBucket.class, "../sift/pivots_2560", 512,
                "../sift/query_1000", 1000, "../sift/data_1M", 1000000,
                "../sift/groundtruth_1M",
                "INFO: ParallelSequentialScan processed: KNNQueryOperation <ObjectFloatVectorL2 \\((\\w\\d+)\\).*",
                "src/main/java/bp/computedPivotCoefs/Sift.csv");
    }
}
