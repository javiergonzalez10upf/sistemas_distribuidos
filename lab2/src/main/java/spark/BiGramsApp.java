package spark;

import edu.upf.model.Bigram;
import edu.upf.model.ExtendedSimplifiedTweet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.*;

import static edu.upf.filter.FileLanguageFilter.filterLanguage;

public class BiGramsApp {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: spark-submit --master <YOUR MASTER> --class spark.BiGramsApp your.jar <language> <output> <inputFile/Folder>");
            System.exit(1);
        }

        String language = args[0];
        String output = args[1];
        String input = args[2];

        // Spark application logic here...
        SparkConf conf = new SparkConf().setAppName("BiGramsApp");
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> RDDallLines_spaces = sc.textFile(input);
        JavaRDD<String> RDDallLines = RDDallLines_spaces.filter(line -> !line.isEmpty());

        JavaRDD<String> allLines = filterLanguage(RDDallLines, language);

        JavaRDD<Optional<ExtendedSimplifiedTweet>> parsed_lines = allLines.map(ExtendedSimplifiedTweet::fromJson);

        JavaRDD<String> text = parsed_lines
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(ExtendedSimplifiedTweet::getText);

        // Convert text RDD to a flat map of bigrams
        JavaPairRDD<Bigram, Integer> flatMapBigrams = text
                .flatMapToPair(sentence -> {
                            String[] words = sentence.split(" ");
                            List<Tuple2<Bigram, Integer>> pairs = new ArrayList<>();

                            String[] nonEmptyWords = Arrays.stream(words)
                                    .map(BiGramsApp::normalise)
                                    .filter(word -> !word.isEmpty())
                                    .toArray(String[]::new);

                            for (int i = 0; i < nonEmptyWords.length - 1; i++) {
                                Bigram bigram = new Bigram(nonEmptyWords[i], nonEmptyWords[i + 1]);
                                pairs.add(new Tuple2<>(bigram, 1));
                            }
                            return pairs.iterator();
                        })
                .reduceByKey(Integer::sum);

        flatMapBigrams.saveAsTextFile(output);

        // Sort by count in descending order
        JavaPairRDD<Bigram, Integer> sortedBigrams = flatMapBigrams
                .mapToPair(tuple -> new Tuple2<>(tuple._2(), tuple._1()))
                .sortByKey(false)
                .mapToPair(tuple -> new Tuple2<>(new Bigram(tuple._2().getWord1(), tuple._2().getWord2()), tuple._1()));


        // Take the top 10 bigrams
        List<Tuple2<Bigram, Integer>> top10Bigrams = sortedBigrams.take(10);

        // Print the top 10 bigrams
        System.out.println("Top 10 Bigrams:");
        for (Tuple2<Bigram, Integer> entry : top10Bigrams) {
            System.out.println(entry._1().getWord1() + " - " + entry._1().getWord2() + ": " + entry._2());
        }

        sc.stop();

        // Save results to output...

    }


    private static List<Bigram> generateBigrams(List<String> words) {
        // Implement logic to generate bigrams from the list of words...
        List<Bigram> bigrams = new ArrayList<>();

        if (words.size() < 2) {
            // Not enough words to generate bigrams
            return bigrams;
        }

        for (int i = 0; i < words.size() - 1; i++) {
            String word1 = words.get(i).trim().toLowerCase();
            String word2 = words.get(i + 1).trim().toLowerCase();

            // Filter out empty words
            if (!word1.isEmpty() && !word2.isEmpty()) {
                Bigram bigram = new Bigram(word1, word2);
                bigrams.add(bigram);
            }
        }

        return bigrams;
    }
    public static String normalise(String word) { return word.trim().toLowerCase();}
}