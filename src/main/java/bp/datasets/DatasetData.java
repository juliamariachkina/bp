package bp.datasets;

import messif.buckets.LocalBucket;
import messif.objects.LocalAbstractObject;

/**
 * An abstract class the sole purpose of which is to encapsulate dataset metadata.
 */
public abstract class DatasetData {
    /**
     * The class that represents data, pivot or query object from this dataset
     **/
    public Class<? extends LocalAbstractObject> objectClass;
    /** The class of a bucket to use with this dataset **/
    public Class<? extends LocalBucket> bucketClass;
    /** The filepath to a file that stores serialized pivots **/
    public String pivotFilePath;
    /** The number of pivots to read from the file stored at pivotFilePath **/
    public int pivotCount;
    /** The filepath to a file that stores serialized queries **/
    public String queryFilePath;
    /**
     * The number of queries to read from the file stored at queryFilePath
     **/
    public int queryCount;
    /**
     * The filepath to a file that stores serialized data objects
     **/
    public String dataFilePath;
    /**
     * The number of data objects to read from the file stored at dataFilePath
     **/
    public int dataObjectsCount;
    /**
     * The filepath to a serialized dataset ground truth file
     **/
    public String groundTruthPath;
    /**
     * The regex pattern for queryURI parsing when reading the ground truth file
     **/
    public String queryPattern;
    /**
     * The filepath to a serialized file with pivot coefficients computed for this dataset's
     * pivots (used by the LAESA with limited angles algorithm)
     **/
    public String pivotCoefsFilePath;

    /**
     * Creates a new instance of DatasetData.
     *
     * @param objectClass        the class that represents data, pivot or query object from this dataset
     * @param bucketClass        the class of a bucket to use with this dataset
     * @param pivotFilePath      the filepath to a file that stores serialized pivots
     * @param pivotCount         the number of pivots to read from the file stored at pivotFilePath
     * @param queryFilePath      the filepath to a file that stores serialized queries
     * @param queryCount         the number of queries to read from the file stored at queryFilePath
     * @param dataFilePath       the filepath to a file that stores serialized data objects
     * @param dataObjectsCount   the number of data objects to read from the file stored at dataFilePath
     * @param groundTruthPath    the filepath to a serialized dataset ground truth file
     * @param queryPattern       the regex pattern for queryURI parsing when reading the ground truth file
     * @param pivotCoefsFilePath the filepath to a serialized file with pivot coefficients computed for this dataset's
     *                           pivots (used by the LAESA with limited angles algorithm)
     */
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
