package bp;

import bp.evaluators.SimpleQueryEvaluator;
import messif.algorithms.impl.ParallelSequentialScan;
import messif.objects.impl.ObjectFloatVectorL2;
import messif.objects.util.DistanceRankedObject;
import messif.objects.util.RankedAbstractObject;
import messif.statistics.StatisticCounter;
import messif.statistics.Statistics;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Main {
    private static final Logger LOG = Logger.getLogger(Main.class.getName());

    public static void main(String args[]) throws IOException {
        SimpleQueryEvaluator<ObjectFloatVectorL2> simpleQueryEvaluator = new SimpleQueryEvaluator<>(
                new ParallelSequentialScan(4),
                "../data/D20_query_objects_uniform_distribution.data",
                10, 5, "../data/D20_data_objects_uniform_distribution.data",
                100000, ObjectFloatVectorL2.class);
        simpleQueryEvaluator.insertData();
        Statistics.resetStatistics();

        Map<String, List<RankedAbstractObject>> result = simpleQueryEvaluator.evaluateQueries();
        StatisticCounter distComp = StatisticCounter.getStatistics("DistanceComputations");

        for (Map.Entry<String, List<RankedAbstractObject>> locatorTo5NN : result.entrySet()) {
            System.out.println("Key: " + locatorTo5NN.getKey() + ", Answer: " +
                    locatorTo5NN.getValue().stream().map(DistanceRankedObject::toString).collect(Collectors.joining(",")));
        }

        CSVWriter writer = new CSVWriter("src/main/java/bp/results/SeqScan.csv");
        writer.writeQueryResults(result, distComp);
    }
}
