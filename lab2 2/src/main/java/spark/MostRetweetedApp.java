package spark;

import edu.upf.model.ExtendedSimplifiedTweet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static edu.upf.filter.FileLanguageFilter.filterLanguage;

public class MostRetweetedApp {
    public static void main(String[] args){
        List<String> argsList = Arrays.asList(args);
        String output = argsList.get(0);
        String input = argsList.get(1);

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("Most Retweeted Tweets for Most Retweeted Users");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> RDDallLines_spaces = sc.textFile(input);
        JavaRDD<String> RDDallLines = RDDallLines_spaces.filter(line -> !line.isEmpty());

        // Read from input
        JavaRDD<ExtendedSimplifiedTweet> extendedSimplifiedTweets = RDDallLines
                .map(ExtendedSimplifiedTweet::fromJson)
                .filter(Optional::isPresent)
                .map(Optional::get);

        // Num retweets per author (retweetedUserId, count)
        JavaPairRDD<Long, Integer> retweetedUserCounts = extendedSimplifiedTweets
                .filter(ExtendedSimplifiedTweet::isRetweeted)
                .mapToPair(tweet -> new Tuple2<>(tweet.getRetweetedUserId(), 1))
                .reduceByKey(Integer::sum);

        //Trobem top 10 users based on the previous count
        List<Tuple2<Long, Integer>> topUsers = retweetedUserCounts
                .mapToPair(tuple -> new Tuple2<>(tuple._2(), tuple._1()))       //we swap key and value for sorting, with _2 we are accesing to count and with _1 to id
                .sortByKey(false)                                               //sort by count in descending order(desc)
                .mapToPair(tuple -> new Tuple2<>(tuple._2(), tuple._1()))       //swap back to original (id, count)
                .take(10);

        // Creem un set del top 10 de users IDs per filtrar desprÃ©s
        Set<Long> topUserIds = topUsers.stream().map(Tuple2::_1).collect(Collectors.toSet());

        // Per per cada user del top10 que ha estat RT, fem una llista amb tots
        // els seus tweets que han estat RT
        // (UserA, [Tweet1A, Tweet2A, Tweet3A])
        // (UserB, [Tweet1B, Tweet2B])
        JavaPairRDD<Long, Iterable<ExtendedSimplifiedTweet>> tweetsGroupedByUser = extendedSimplifiedTweets
                .filter(ExtendedSimplifiedTweet::isRetweeted)
                .groupBy(ExtendedSimplifiedTweet::getRetweetedUs