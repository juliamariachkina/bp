package bp.datasets;

import messif.buckets.LocalBucket;
import messif.buckets.impl.MemoryStorageBucket;
import messif.objects.impl.ObjectFloatVectorL2;

public class DecafData extends DatasetData {
    public Class<?> objectClass = ObjectFloatVectorL2.class;
    public Class<? extends LocalBucket> bucketClass = MemoryStorageBucket.class;
    public String pivotFilePath = "../decaf/pivots_2560";
    public int pivotCount = 256;
    public String queryFilePath = "../decaf/query_1000";
    public int queryCount = 1000;
    public String dataFilePath = "../decaf/data_1M.gz";
    public int dataObjectsCount = 1000000;
    public String groundTruthPath = "../decaf/groundtruth_q1000";
    public String queryPattern = "query=(\\d+)";
}
