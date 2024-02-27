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
        List<String> inputFilesList = Arrays.asList(Arrays.copyOfRange(args, 2, args.length));

        String inputFiles = String.join(",", inputFilesList);

        // Spark application logic here...
        SparkConf conf = new SparkConf().setAppName("BiGramsApp");
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> RDDallLines_spaces = sc.textFile(inputFiles);
        JavaRDD<String> RDDallLines = RDDallLines_spaces.filter(line -> !line.isEmpty());

        JavaRDD<String> allLines = filterLanguage(RDDallLines, language);

        JavaRDD<Optional<ExtendedSimplifiedTweet>> parsed_lines = allLines.map(ExtendedSimplifiedTweet::fromJson);
        // Aqí, tenemos una RDD con los textos de los tweets que son del lenguaje deseado, y, que están bien (ispresent)
        JavaRDD<String> text = parsed_lines
                .filter(Optional::isPresent)
                .map(Optional::get)//.filter(tweet ->{ return !tweet.isRetweeted();})
                .map(ExtendedSimplifiedTweet::getText);

        JavaRDD<Tuple2<String, String>> bigrams = text
                .flatMap(sentence -> {
                    List<String> words = Arrays.asList(sentence.split(" "));
                    List<Tuple2<String, String>> list_bigrams = generateBigrams(words);
                    return list_bigrams.iterator();
                });
        System.out.println("\n\n\nbigrams original\n\n");
        bigrams.foreach(tuple -> {
            String element1 = tuple._1();
            String element2 = tuple._2();
            System.out.println(element1 + ", " + element2);
        });

        JavaPairRDD<Tuple2<String, String>, Integer> pairedBigrams = bigrams.mapToPair(bigram -> new Tuple2<>(bigram, 1));
        System.out.println("\n\n\nbigrams con un uno al lado\n\n\n");
        pairedBigrams.foreach(tuple -> {
            Tuple2<String, String> key = tuple._1();
            Integer count = tuple._2();
            System.out.println(key._1() + ", " + key._2() + ", " + count);
        });
        JavaPairRDD<Tuple2<String, String>, Integer> ReducedBigrams = pairedBigrams.reduceByKey(Integer::sum);
        System.out.println("reduced bigrams no order nothing");
        ReducedBigrams.foreach(tuple -> {
            Tuple2<String, String> key = tuple._1();
            Integer count = tuple._2();
            System.out.println(key._1() + ", " + key._2() + ", " + count);
        });

        JavaPairRDD<Integer, Tuple2<String, String>> SwappedBigrams = ReducedBigrams.mapToPair(Tuple2::swap);

        JavaPairRDD<Integer, Tuple2<String, String>> SortedBigrams = SwappedBigrams.sortByKey(false);

        JavaPairRDD<Tuple2<String, String>, Integer> SortSwapBigrams = SortedBigrams.mapToPair(Tuple2::swap);
        System.out.println("sorted bigrams");
        SortSwapBigrams.foreach(tuple -> {
            Tuple2<String, String> key = tuple._1();
            Integer count = tuple._2();
            System.out.println(key._1() + ", " + key._2() + ", " + count);
        });

        // Obtener las 10 primeras entradas
        List<Tuple2<Tuple2<String, String>, Integer>> Tuple2first10Entries = SortSwapBigrams.take(10);

        // Crear una nueva RDD a partir de la lista
        JavaPairRDD<Tuple2<String, String>, Integer> top10bigrams = sc.parallelizePairs(Tuple2first10Entries);
        System.out.println("top10");
        top10bigrams.foreach(tuple -> {
            Tuple2<String, String> key = tuple._1();
            Integer count = tuple._2();
            System.out.println(key._1() + ", " + key._2() + ", " + count);
        });

        top10bigrams.saveAsTextFile(output);

/*
        // Obtener las 10 entradas con mayor valor entero
        //List<Tuple2<Bigram, Integer>> top10Bigrams = countedBigrams.takeOrdered(10, (tuple1, tuple2) -> -Integer.compare(tuple1._2(), tuple2._2()));

        // Guardar las 10 entradas con mayor valor entero en un archivo
        //sc.parallelize(top10Bigrams).saveAsTextFile(output);
        JavaRDD<String> top10Bigrams = countedBigrams
                .mapToPair(Tuple2::swap) // Intercambiar clave y valor para que los valores sean claves
                .sortByKey(false) // Ordenar por clave en orden descendente
                .mapToPair(Tuple2::swap) // Intercambiar nuevamente para restaurar la forma original
                .zipWithIndex() // Agregar índices a los elementos
                .filter(tuple -> tuple._2() < 10) // Filtrar los primeros 10 elementos
                .map(tuple -> {
                    Bigram bigram = tuple._1();
                    Long count = tuple._2();
                    return String.format("%s, %s, %d", bigram.getWord1(), bigram.getWord2(), count);

        });

// Guardar el resultado o realizar otras operaciones con top10Bigrams
        top10Bigrams.saveAsTextFile(output);



// Guardar el resultado o realizar otras operaciones con top10Bigrams
        //top10Bigrams.saveAsTextFile(output);


        //for (Tuple2<Bigram, Integer> entry : countedBigrams.collect()) {
        //    System.out.println(entry._1().getWord1() + " - " + entry._1().getWord2() + ": " + entry._2());
        //}

/*
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
                        });

        JavaPairRDD<Bigram, Integer> reducedBigrams = flatMapBigrams.reduceByKey(Integer::sum);

        System.out.println("Top Bigrams:");
        for (Tuple2<Bigram, Integer> entry : reducedBigrams.collect()) {
            System.out.println(entry._1().getWord1() + " - " + entry._1().getWord2() + ": " + entry._2());
        }


        System.out.println("Top 10 Bigrams:");
        for (Tuple2<Bigram, Integer> entry : flatMapBigrams) {
            System.out.println(entry._1().getWord1() + " - " + entry._1().getWord2() + ": " + entry._2());
        }

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
        }*/

        sc.stop();

        // Save results to output...

    }


    private static List<Tuple2<String, String>> generateBigrams(List<String> words) {
        // Implement logic to generate bigrams from the list of words...
        List<Tuple2<String, String>> bigrams = new ArrayList<>();

        if (words.size() < 2) {
            // Not enough words to generate bigrams
            return bigrams;
        }

        for (int i = 0; i < words.size() - 1; i++) {
            String word1 = words.get(i).trim().toLowerCase();
            String word2 = words.get(i + 1).trim().toLowerCase();

            // Filter out empty words
            if (!word1.isEmpty() && !word2.isEmpty()) {
                Tuple2<String, String> bigram = new Tuple2<>(word1, word2);
                bigrams.add(bigram);
            }
        }

        return bigrams;
    }
    public static String normalise(String word) { return word.trim().toLowerCase();}
}