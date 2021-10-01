package bp.datasets;

import messif.buckets.impl.MemoryStorageBucket;
import messif.objects.impl.ObjectFloatVectorL2;

public class RandomData extends DatasetData {

    public RandomData() {
        super(ObjectFloatVectorL2.class, MemoryStorageBucket.class, "../random/pivots_256", 256,
                "../random/query_1000", 10, "../random/data_1M", 1000000,
                "../random/groundtruth_1000", "INFO: Algorithm processed: RangeQueryOperation <ObjectFloatVectorL2 \\((\\w\\d+)\\), .*");
    }
}
