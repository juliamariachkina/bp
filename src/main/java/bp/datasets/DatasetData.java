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
    public String pivotCoefsFilePath;

    public DatasetData(Class<? extends LocalAbstractObject> objectClass,
                       Class<? extends LocalBucket> bucketClass,
                       String pivotFilePath, int pivotCount, String queryFilePath,
                       int queryCount, String dataFilePath, int dataObjectsCount,
                       String groundTruthPath, String queryPattern, String pivotCoefsFilePath) {
        this.objectClass = objectClass;
        this.bucketClass = bucketClass;
        this.pivotFilePath = pivotFilePath;
        this.pivotCount = pivotCount;
        this.queryFilePath = queryFilePath;
        this.queryCount = queryCount;
        this.dataFilePath = dataFilePath;
        this.dataObjectsCount = dataObjectsCount;
        this.groundTruthPath = groundTruthPath;
        this.queryPattern = queryPattern;
        this.pivotCoefsFilePath = pivotCoefsFilePath;
    }
}
