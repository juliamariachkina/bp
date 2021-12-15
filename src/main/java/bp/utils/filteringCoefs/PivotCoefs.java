package bp.utils.filteringCoefs;

import bp.CSVWriter;
import bp.datasets.DatasetData;
import bp.utils.Utility;
import messif.objects.LocalAbstractObject;
import messif.objects.util.AbstractObjectIterator;
import messif.objects.util.AbstractObjectList;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * This class computes the pivot coefficients that are used by the LAESA with an assumption of
 * limited angles in triangles of distances algorithm.
 */
public class PivotCoefs {

    private static final Logger LOG = Logger.getLogger(PivotCoefs.class.getName());
    private final DatasetData datasetData;

    public PivotCoefs(DatasetData dataset) {
        datasetData = dataset;
    }

    /**
     * Computes pairwise distances between the objects form list from and objects from list to.
     *
     * @param from first list of objects
     * @param to   second list of objects
     * @param <T>  type of objects from the list from
     * @param <S>  type of objects form the list to
     * @return map with object URIs (1) from list from as keys and nested maps as values. The nested map has
     * an object URI (2) from list to as a key and a distance between the (1) and (2) as a value
     */
    private <T extends LocalAbstractObject, S extends LocalAbstractObject> Map<String, Map<String, Float>> computeDistances(
            List<T> from, List<S> to) {
        Map<String, Map<String, Float>> fromURItoMaptoURItoDistance = new HashMap<>();
        for (LocalAbstractObject fromObject : from) {
            for (LocalAbstractObject toObject : to) {
                float distance = toObject.getDistance(fromObject);
                fromURItoMaptoURItoDistance.putIfAbsent(fromObject.getLocatorURI(), new HashMap<>());
                fromURItoMaptoURItoDistance.get(fromObject.getLocatorURI()).put(toObject.getLocatorURI(), distance);
            }
        }
        return fromURItoMaptoURItoDistance;
    }

    /**
     * Computes the 0.4% of the smallest distances between objects from list from and objects from list to.
     *
     * @param from first list of objects
     * @param to   second list of objects
     * @param <T>  type of objects from the list from
     * @param <S>  type of objects form the list to
     * @return map with object URIs (1) from list from as keys and nested maps as values. The nested map has
     * an object URI (2) from list to as a key and a distance between the (1) and (2) as a value. This map contains
     * 0.4% of the smallest distances between objects from list from and objects from list to
     */
    private <T extends LocalAbstractObject, S extends LocalAbstractObject> Map<String, Map<String, Float>> computeSmallestDistances(
            List<T> from, List<S> to) {
        Map<String, Map<String, Float>> fromURItoMaptoURItoDistance = new HashMap<>();
        Map<Float, List<String>> smallestDistancesTofromURI = new HashMap<>();
        float maxDistance = 0;
        long requiredSize = (long) (0.004 * from.size() * to.size());
        long count = 0;
        for (LocalAbstractObject fromObject : from) {
            for (LocalAbstractObject toObject : to) {
                float distance = toObject.getDistance(fromObject);
                if (count == requiredSize && distance >= maxDistance)
                    continue;

                fromURItoMaptoURItoDistance.putIfAbsent(fromObject.getLocatorURI(), new HashMap<>());
                fromURItoMaptoURItoDistance.get(fromObject.getLocatorURI()).put(toObject.getLocatorURI(), distance);
                smallestDistancesTofromURI.putIfAbsent(distance, new ArrayList<>());
                smallestDistancesTofromURI.get(distance).add(fromObject.getLocatorURI());

                if (count == requiredSize && distance < maxDistance) {
                    List<String> fromURIs = smallestDistancesTofromURI.get(maxDistance);
                    String removedFromURI = fromURIs.get(fromURIs.size() - 1);
                    fromURIs.remove(fromURIs.size() - 1);
                    if (fromURIs.isEmpty())
                        smallestDistancesTofromURI.remove(maxDistance);

                    float finalMaxDistance = maxDistance;
                    Map<String, Float> toURItoDistance = fromURItoMaptoURItoDistance.get(removedFromURI);
                    String key = toURItoDistance.entrySet().stream()
                            .filter(entry -> entry.getValue().equals(finalMaxDistance))
                            .findFirst().get().getKey();
                    toURItoDistance.remove(key);
                    if (toURItoDistance.isEmpty())
                        fromURItoMaptoURItoDistance.remove(removedFromURI);
                    maxDistance = smallestDistancesTofromURI.keySet().stream().reduce(0f, (a, b) -> a > b ? a : b);
                    --count;
                }
                ++count;
                maxDistance = Math.max(maxDistance, distance);
            }
        }
        return fromURItoMaptoURItoDistance;
    }

    /**
     * Computes pivot coefficients of all pivots of the dataset identified by the DatasetData parameter provided to
     * the constructor of the class. The pivot coefficients are computed according to the algorithm from the paper named
     * "Data-Dependent Metric Filtering" by Vladimir Mic and Pavel Zezula.
     *
     * @param filePath file path to the file where to store the computed pivot coefficients
     * @throws IOException propagates the exception
     */
    public void computePivotCoefs(String filePath) throws IOException {
        List<? extends LocalAbstractObject> pivots = Utility.getObjectsList(
                datasetData.pivotFilePath, datasetData.objectClass, datasetData.pivotCount);
        AbstractObjectIterator<? extends LocalAbstractObject> objectIter = Utility.getObjectsIterator(
                datasetData.dataFilePath, datasetData.objectClass); // File with dataset objects contains all: objects, queries and pivots
        AbstractObjectList<? extends LocalAbstractObject> objects = objectIter.getRandomObjects(
                11000 + datasetData.pivotCount, true);
        pivots.forEach(pivot ->
                objects.removeIf(object -> object.dataEquals(pivot)
                        || object.getLocatorURI().equals(pivot.getLocatorURI()))); // To avoid having a pivot as an object
        List<? extends LocalAbstractObject> queries = objects.provideObjects().getRandomObjects(1000, true);
        queries.forEach(query ->
                objects.removeIf(object -> object.dataEquals(query)
                        || object.getLocatorURI().equals(query.getLocatorURI()))); // To avoid having a query as an object
        List<? extends LocalAbstractObject> objectsList = objects.stream().limit(10000).collect(Collectors.toList());


        Map<String, Map<String, Float>> objectURItoMapQueryURItoDistance = computeSmallestDistances(objectsList, queries);
        List<? extends LocalAbstractObject> filteredObjects = objectsList.stream()
                .filter(object -> objectURItoMapQueryURItoDistance.containsKey(object.getLocatorURI()))
                .collect(Collectors.toList());
        Set<String> filteredQueriesURIs = objectURItoMapQueryURItoDistance.values().stream()
                .map(Map::keySet).reduce(new HashSet<>(), (a, b) -> {
                    a.addAll(b);
                    return a;
                });
        List<? extends LocalAbstractObject> filteredQueries = queries.stream()
                .filter(query -> filteredQueriesURIs.contains(query.getLocatorURI()))
                .collect(Collectors.toList());

        Map<String, Map<String, Float>> objectURItoMapPivotURItoDistance = computeDistances(filteredObjects, pivots);
        Map<String, Map<String, Float>> queryURItoMapPivotURItoDistance = computeDistances(filteredQueries, pivots);

        Map<String, Float> pivotURItoCoef = new HashMap<>();
        for (LocalAbstractObject pivot : pivots) { // Process 0.4% of triangles p,q,o with smallest d(q,o)
            float coefForP = Float.MAX_VALUE;
            for (Map.Entry<String, Map<String, Float>> objectURItoMapQueryURItoDistEntry : objectURItoMapQueryURItoDistance.entrySet()) {
                for (Map.Entry<String, Float> queryURItoDist : objectURItoMapQueryURItoDistEntry.getValue().entrySet()) {
                    float objectToQueryDist = queryURItoDist.getValue();
                    float objectToPivotDist = objectURItoMapPivotURItoDistance.get(objectURItoMapQueryURItoDistEntry.getKey())
                            .get(pivot.getLocatorURI());
                    float queryToPivotDist = queryURItoMapPivotURItoDistance.get(queryURItoDist.getKey()).get(pivot.getLocatorURI());
                    if (objectToPivotDist == queryToPivotDist && objectToQueryDist == queryToPivotDist) // If the triangle is equilateral, skip it
                        continue;
                    if (objectURItoMapQueryURItoDistEntry.getKey().equals(pivot.getLocatorURI())
                            || queryURItoDist.getKey().equals(pivot.getLocatorURI())) // In a triangle all objects have to be unique
                        throw new IllegalArgumentException(
                                (objectURItoMapQueryURItoDistEntry.getKey().equals(pivot.getLocatorURI()) ? "Object" : "Query") +
                                        " URI is the same as the pivot URI for " + pivot.getLocatorURI());
                    if (objectURItoMapQueryURItoDistEntry.getKey().equals(queryURItoDist.getKey())) // In a triangle all objects have to be unique
                        throw new IllegalArgumentException("Object URI equals query URI for " + queryURItoDist.getKey());
                    float a = Math.min(objectToPivotDist, queryToPivotDist);
                    float b = Math.max(objectToPivotDist, queryToPivotDist);
                    float c = objectToQueryDist;
                    float coef = c / (b - a);
                    if (c == 0 || a == 0 || b == 0) { // There should not be 0 distances in triangles of unique objects
                        LocalAbstractObject o = filteredObjects.stream()
                                .filter(object -> object.getLocatorURI().equals(objectURItoMapQueryURItoDistEntry.getKey()))
                                .findFirst().get();
                        LocalAbstractObject q = filteredQueries.stream()
                                .filter(query -> query.getLocatorURI().equals(queryURItoDist.getKey()))
                                .findFirst().get();
                        throw new IllegalArgumentException("Distances in the triangles can't be 0, but they are for pivot "
                                + pivot.getLocatorURI() + " query " + q.getLocatorURI() + " object " + o.getLocatorURI());
                    }
                    coefForP = Math.min(coef, coefForP);
                }
            }
            pivotURItoCoef.put(pivot.getLocatorURI(), coefForP);
        }
        CSVWriter.writePivotCoefs(pivotURItoCoef, filePath);
    }
}
