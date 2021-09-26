package bp.datasets;

import messif.buckets.impl.MemoryStorageBucket;
import messif.objects.impl.MetaObjectSAPIRWeightedDist2;

public class MpegData extends DatasetData {

    public MpegData() {
        super(MetaObjectSAPIRWeightedDist2.class, MemoryStorageBucket.class, "../mpeg/pivots_2560", 256,
                "../mpeg/query_1000", 1000, "../mpeg/data_1M", 1000000,
                "../mpeg/groundtruth_q1000",
                "INFO: Algorithm processed: KNNQueryOperation <MetaObjectSAPIRWeightedDist3 \\((\\d+)\\).*");
    }
}
