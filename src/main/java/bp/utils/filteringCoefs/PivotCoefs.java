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

public class PivotCoefs {
    private static final Logger LOG = Logger.getLogger(PivotCoefs.class.getName());
    private final DatasetData datasetData;

    public PivotCoefs(DatasetData dataset) {
        datasetData = dataset;
    }

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

    public void computePivotCoefs(String filePath) throws IOException {
        List<? extends LocalAbstractObject> pivots = Utility.getObjectsList(
                datasetData.pivotFilePath, datasetData.objectClass, datasetData.pivotCount);
        AbstractObjectIterator<? extends LocalAbstractObject> objectIter = Utility.getObjectsIterator(
                datasetData.dataFilePath, datasetData.objectClass); // Dataset objects file contains all: objects, queries and pivots
        AbstractObjectList<? extends LocalAbstractObject> objects = objectIter.getRandomObjects(1356, true);
        objects.removeAll(pivots); // To avoid having pivot as an object
        if (objects.size() == 11256)
            throw new IllegalArgumentException("No pivots were removed from objects");
        List<? extends LocalAbstractObject> queries = objects.provideObjects().getRandomObjects(100, true);
        objects.removeAll(queries); // To avoid having query as an object
        if (objects.size() > 11000)
            throw new IllegalArgumentException("No queries were removed from objects");
        List<? extends LocalAbstractObject> objectsList = objects.stream().limit(1000).collect(Collectors.toList());


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
        for (LocalAbstractObject pivot : pivots) {
            float coefForP = Float.MAX_VALUE;
            for (Map.Entry<String, Map<String, Float>> objectURItoMapQueryURItoDistEntry : objectURItoMapQueryURItoDistance.entrySet()) {
                for (Map.Entry<String, Float> queryURItoDist : objectURItoMapQueryURItoDistEntry.getValue().entrySet()) {
                    float objectToQueryDist = queryURItoDist.getValue();
                    float objectToPivotDist = objectURItoMapPivotURItoDistance.get(objectURItoMapQueryURItoDistEntry.getKey())
                            .get(pivot.getLocatorURI());
                    float queryToPivotDist = queryURItoMapPivotURItoDistance.get(queryURItoDist.getKey()).get(pivot.getLocatorURI());
                    if (objectToPivotDist == queryToPivotDist && objectToQueryDist == queryToPivotDist)
                        continue;
                    if (objectURItoMapQueryURItoDistEntry.getKey().equals(pivot.getLocatorURI()) || queryURItoDist.getKey().equals(pivot.getLocatorURI()))
                        throw new IllegalArgumentException(
                                (objectURItoMapQueryURItoDistEntry.getKey().equals(pivot.getLocatorURI()) ? "Object" : "Query") +
                                        " URI is the same as the pivot URI for " + pivot.getLocatorURI());
                    float a = Math.min(Math.min(objectToPivotDist, objectToQueryDist), queryToPivotDist);
                    float b = Math.max(Math.max(objectToPivotDist, objectToQueryDist), queryToPivotDist);
                    float c = objectToPivotDist + objectToQueryDist + queryToPivotDist - a - b;
                    float coef = c / (b - a);
                    coefForP = Math.min(coef, coefForP);
                }
            }
            pivotURItoCoef.put(pivot.getLocatorURI(), coefForP);
        }
        CSVWriter.writePivotCoefs(pivotURItoCoef, filePath);
    }
}
