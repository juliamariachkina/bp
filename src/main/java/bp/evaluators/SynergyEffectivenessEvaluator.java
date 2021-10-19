package bp.evaluators;

import bp.CSVWriter;
import bp.datasets.DatasetData;
import bp.parsers.UnfilteredObjectsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

public class SynergyEffectivenessEvaluator {
    private static final Logger LOG = Logger.getLogger(SynergyEffectivenessEvaluator.class.getName());
    private String filePathToStoreResults;
    private DatasetData datasetData;
    private String[] csvFilePaths;

    public SynergyEffectivenessEvaluator(String filePathToStoreResults, DatasetData datasetData, String[] csvFilePaths) {
        this.filePathToStoreResults = filePathToStoreResults;
        this.datasetData = datasetData;
        this.csvFilePaths = csvFilePaths;
    }

    public void evaluateSynergyEffectiveness() throws IOException {
        List<Map<String, Set<String>>> queryURIsToObjectURIsList = new ArrayList<>();
        for (String csvFilePath : csvFilePaths) {
            queryURIsToObjectURIsList.add(new UnfilteredObjectsParser(csvFilePath).parse());
        }

        Map<String, Set<String>> queryURIsToIntersectionObjectURIs = queryURIsToObjectURIsList.stream().reduce(
                (a, b) -> {
                    a.forEach((key, value) -> value.retainAll(b.get(key)));
                    return a;
                }).get();

        CSVWriter.writeSynergyEffectiveness(datasetData, filePathToStoreResults, queryURIsToIntersectionObjectURIs);
    }
}
