package bp.datasets;

import messif.buckets.impl.MemoryStorageBucket;
import messif.objects.impl.ObjectFloatVectorL2;

public class DecafData extends DatasetData {

    public DecafData() {
        super(ObjectFloatVectorL2.class, MemoryStorageBucket.class, "../decaf/pivots_2560", 512,
                "../decaf/query_1000", 1000, "../decaf/data_1M.gz", 1000000,
                "../decaf/groundtruth_q1000", "query=(\\d+)");
    }
}
