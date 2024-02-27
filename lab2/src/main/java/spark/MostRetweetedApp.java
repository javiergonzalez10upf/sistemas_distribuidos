package spark;

import edu.upf.model.ExtendedSimplifiedTweet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static edu.upf.filter.FileLanguageFilter.filterLanguage;

public class MostRetweetedApp {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: spark-submit --master <YOUR MASTER> --class spark.MostRetweetedApp your.jar <output> <inputFile/Folder>");
            System.exit(1);
        }

        String output = args[0];
        String input = args[1];

        // Spark application logic here...
        SparkConf conf = new SparkConf().setAppName("MostRetweetedApp");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read input and create an RDD of ExtendedSimplifiedTweet
        JavaRDD<String> RDDallLines_spaces = sc.textFile(input);
        JavaRDD<String> RDDallLines = RDDallLines_spaces.filter(line -> !line.isEmpty());

        JavaRDD<Optional<ExtendedSimplifiedTweet>> parsed_lines = RDDallLines.map(ExtendedSimplifiedTweet::fromJson);

        // Filter out tweets that are not retweets
        JavaRDD<ExtendedSimplifiedTweet> retweetsRDD = parsed_lines
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(ExtendedSimplifiedTweet::isRetweeted);

        // Identify the most retweeted users
        JavaPairRDD<Long, Integer> mostRetweetedUsers = retweetsRDD
                .flatMapToPair(tweet -> {
                    Long retweetedUserId = tweet.getRetweetedUserId();
                    return Arrays.asList(new Tuple2<>(retweetedUserId, 1)).iterator();
                }).reduceByKey(Integer::sum);

        List<Tuple2<Integer, Long>> topUsersRetweeted = mostRetweetedUsers.mapToPair(Tuple2::swap).sortByKey(false).take(10);

        JavaRDD<ExtendedSimplifiedTweet> topTweets = retweetsRDD
                .filter(tweet -> {
                    long userId = tweet.getUserId();
                    return topUsersRetweeted.stream().anyMatch(tuple -> tuple._2() == userId);
                });


        // Extract most retweeted tweets for each user
        //JavaRDD<ExtendedSimplifiedTweet> mostRetweetedTweets = mostRetweetedUsers
        //        .flatMap(userIdTweetId -> retweetsRDD
        //                .filter(tweet -> tweet.getUserId() == userIdTweetId && tweet.getTweetId() == mostRetweetedTweetId)
        //        );

        // Save the results
        topTweets.saveAsTextFile(output);
        topTweets.foreach(tweet -> System.out.println("Tweet: " + tweet));
        sc.stop();

    }

}
