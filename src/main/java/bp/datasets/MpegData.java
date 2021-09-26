package bp.datasets;

import messif.buckets.LocalBucket;
import messif.buckets.impl.MemoryStorageBucket;
import messif.objects.impl.MetaObjectSAPIRWeightedDist2;

public class MpegData extends DatasetData {
    public Class<?> objectClass = MetaObjectSAPIRWeightedDist2.class;
    public Class<? extends LocalBucket> bucketClass = MemoryStorageBucket.class;
    public String pivotFilePath = "../mpeg/pivots_2560";
    public int pivotCount = 256;
    public String queryFilePath = "../mpeg/query_1000";
    public int queryCount = 1000;
    public String dataFilePath = "../mpeg/data_1M";
    public int dataObjectsCount = 1000000;
    public String groundTruthPath = "../mpeg/groundtruth_q1000";
    public String queryPattern = "INFO: Algorithm processed: KNNQueryOperation <MetaObjectSAPIRWeightedDist3 \\((\\d+)\\).*";
}
