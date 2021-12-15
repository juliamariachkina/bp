package bp.datasets;

import messif.buckets.impl.MemoryStorageBucket;
import messif.objects.impl.MetaObjectSAPIRWeightedDist2;

/**
 * This class stores metadata of the MPEG7 dataset.
 */
public class MpegData extends DatasetData {

    /**
     * Creates a new instance of MpegData by passing all the required parameters to its ancestor DatasetData.
     */
    public MpegData() {
        super(MetaObjectSAPIRWeightedDist2.class, MemoryStorageBucket.class, "../mpeg/pivots_2560", 512,
                "../mpeg/query_1000", 1000, "../mpeg/data_1M.gz", 1000000,
                "../mpeg/groundtruth_q1000",
                "INFO: Algorithm processed: KNNQueryOperation <MetaObjectSAPIRWeightedDist2 \\((\\d+)\\).*",
                "src/main/java/bp/computedPivotCoefs/Mpeg.csv");
    }
}
