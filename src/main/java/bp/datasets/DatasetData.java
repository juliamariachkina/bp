package bp.datasets;

import messif.buckets.LocalBucket;
import messif.objects.LocalAbstractObject;

public abstract class DatasetData {
    public Class<? extends LocalAbstractObject> objectClass;
    public Class<? extends LocalBucket> bucketClass;
    public String pivotFilePath;
    public int pivotCount;
    public String queryFilePath;
    public int queryCount;
    public String dataFilePath;
    public int dataObjectsCount;
    public String groundTruthPath;
    public String queryPattern;
}
