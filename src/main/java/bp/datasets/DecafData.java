package bp.datasets;

import messif.buckets.impl.MemoryStorageBucket;
import messif.objects.impl.ObjectFloatVectorL2;

/**
 * This class stores metadata of the DeCAF dataset.
 */
public class DecafData extends DatasetData {

    /**
     * Creates a new instance of DecafData by passing all the required parameters its the ancestor DatasetData.
     */
    public DecafData() {
        super(ObjectFloatVectorL2.class, MemoryStorageBucket.class, "../decaf/pivots_2560", 512,
                "../decaf/query_1000", 1000, "../decaf/data_1M.gz", 1000000,
                "../decaf/groundtruth_q1000", "query=(\\d+)", "src/main/java/bp/computedPivotCoefs/Decaf.csv");
    }
}
