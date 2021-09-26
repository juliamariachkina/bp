package bp.datasets;

import messif.buckets.LocalBucket;
import messif.buckets.impl.MemoryStorageBucket;
import messif.objects.impl.ObjectFloatVectorL2;

public class SiftData extends DatasetData {
    public Class<?> objectClass = ObjectFloatVectorL2.class;
    public Class<? extends LocalBucket> bucketClass = MemoryStorageBucket.class;
    public String pivotFilePath = "../sift/pivots_256";
    public int pivotCount = 256;
    public String queryFilePath = "../sift/query_1000";
    public int queryCount = 1000;
    public String dataFilePath = "../sift/data_1M";
    public int dataObjectsCount = 1000000;
    public String groundTruthPath = "../sift/groundtruth_1M";
    public String queryPattern = "INFO: ParallelSequentialScan processed: KNNQueryOperation <ObjectFloatVectorL2 \\((\\w\\d+)\\).*";
}
