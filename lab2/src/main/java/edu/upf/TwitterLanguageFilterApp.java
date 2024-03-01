package edu.upf;

import edu.upf.uploader.S3Uploader;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import edu.upf.filter.FileLanguageFilter;
import software.amazon.awssdk.regions.Region;
//import edu.upf.uploader.SparkS3Uploader;
import edu.upf.model.SimplifiedTweet;

import java.util.*;



import static edu.upf.filter.FileLanguageFilter.filterLanguage;

public class TwitterLanguageFilterApp {
    public static void main(String[] args) {

        long start = System.currentTimeMillis();

        // verify if the arguments are ok
        if (args.length < 3) {
            System.err.println("Usage: TwitterLanguageFilterApp <language> <outputFile> <inputFile1> <inputFile2> ...");
            System.exit(1);
        }

        // Obtain args
        String language = args[0];
        String outputFile = args[1];
        List<String> inputFilesList = Arrays.asList(Arrays.copyOfRange(args, 2, args.length));

        String inputFiles = String.join(",", inputFilesList);
        //"path1, path2, path3,..."

        // Configuración de Spark
        SparkConf conf = new SparkConf().setAppName("TwitterLanguageFilterApp");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Procesamiento de archivos usando Spark

        JavaRDD<String> RDDallLines_spaces = sc.textFile(inputFiles);
        JavaRDD<String> RDDallLines = RDDallLines_spaces.filter(line -> !line.isEmpty());

        //to filter language

        JavaRDD<String> allLines = RDDallLines.filter(line -> {
            Optional<SimplifiedTweet> optionalTweet = SimplifiedTweet.fromJson(line);
            return optionalTweet.isPresent() && optionalTweet.get().getLanguage().equalsIgnoreCase(language);
        });

        //JavaRDD<String> allLines = filterLanguage(RDDallLines, language);



        // Guardar el resultado en un único archivo local
        allLines.saveAsTextFile(outputFile);

        // Subir el resultado a S3
        //final S3Uploader uploader = new S3Uploader(bucket, language, Region.US_EAST_1);
        //uploader.upload(Arrays.asList(outputFile));

        long end = System.currentTimeMillis();
        System.out.println("\nTime spent: " + (end-start));
        sc.stop();
    }
}