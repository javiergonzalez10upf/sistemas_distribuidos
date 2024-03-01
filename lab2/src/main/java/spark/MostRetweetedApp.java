package spark;

import java.util.Arrays;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import edu.upf.model.ExtendedSimplifiedTweet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import static edu.upf.filter.FileLanguageFilter.filterLanguage;

public class MostRetweetedApp {
    public static void main(String[] args) {
        List<String> argsList = Arrays.asList(args);
        String output = argsList.get(0);

        List<String> inputFilesList = Arrays.asList(Arrays.copyOfRange(args, 1, args.length));

        String inputFiles = String.join(",", inputFilesList);

        // Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("Most Retweeted Tweets for Most Retweeted Users");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> allLinesSpaces = sc.textFile(inputFiles);
        JavaRDD<String> allLines = allLinesSpaces.filter(line -> !line.isEmpty());

        // Read from input
        JavaRDD<ExtendedSimplifiedTweet> ParsedExtendedSimplifiedTweets = allLines
                .map(ExtendedSimplifiedTweet::fromJson)
                .filter(Optional::isPresent)
                .map(Optional::get);

        // Tuple2 of retweetedUserId and count
        JavaPairRDD<Long, Integer> retweetedUserId_count = ParsedExtendedSimplifiedTweets
                .filter(ExtendedSimplifiedTweet::isRetweeted)
                .mapToPair(tweet -> new Tuple2<>(tweet.getRetweetedUserId(), 1))
                .reduceByKey(Integer::sum);

        // Top 10 users based on the previous count (Tuple2 of retweetedUserId and count )
        List<Tuple2<Long, Integer>> top10Users = retweetedUserId_count
                .mapToPair(tuple -> new Tuple2<>(tuple._2(), tuple._1()))
                .sortByKey(false)
                .mapToPair(tuple -> new Tuple2<>(tuple._2(), tuple._1()))
                .take(10);

        // Initialise a set for the top 10 user IDs
        Set<Long> top10UserIds = top10Users.stream().map(Tuple2::_1).collect(Collectors.toSet());

        // List of all the retweeted tweets for each user in the previous top 10.
        JavaPairRDD<Long, Iterable<ExtendedSimplifiedTweet>> RetweetsByUser = ParsedExtendedSimplifiedTweets
                .filter(ExtendedSimplifiedTweet::isRetweeted)
                .groupBy(ExtendedSimplifiedTweet::getRetweetedUserId)
                .filter(tuple -> top10UserIds.contains(tuple._1()));

        // ((UserX, TweetY), CountY)
        JavaRDD<Tuple2<Tuple2<Long, Long>, Integer>> user_Tweet_Counts = RetweetsByUser.flatMap(tuple -> {
            Long userId = tuple._1();
            Iterable<ExtendedSimplifiedTweet> tweets = tuple._2();

            List<Tuple2<Tuple2<Long, Long>, Integer>> user_Tweet_Count_List = new ArrayList<>();

            // Iterate over each tweet for the current user
            for (ExtendedSimplifiedTweet tweet : tweets) {
                Long tweetId = tweet.getRetweetedTweetId();
                int count = 1; // Initialize count to 1 for the current tweet
                Tuple2<Long, Long> userTweetPair = new Tuple2<>(userId, tweetId);
                Tuple2<Tuple2<Long, Long>, Integer> userTweetCount = new Tuple2<>(userTweetPair, count);
                user_Tweet_Count_List.add(userTweetCount);
            }
            return user_Tweet_Count_List.iterator();
        });
        // (userId,tweet), count)

        // Build a JavaPairRDD with ((userId, tweetId), count)
        JavaPairRDD<Tuple2<Long, Long>, Integer> user_Tweet_Counts_PairRDD = user_Tweet_Counts.mapToPair(tuple -> new Tuple2<>(tuple._1(), tuple._2()));

        // ReduceByKey to sum counts for each (user, tweet) tuple
        JavaPairRDD<Tuple2<Long, Long>, Integer> user_tweet = user_Tweet_Counts_PairRDD.reduceByKey(Integer::sum);

        JavaPairRDD<Long, Tuple2<Long, Integer>> user_Tweet_MaxCount_PairRDD = user_tweet.mapToPair(tuple -> {
            Tuple2<Long, Long> userTweetPair = tuple._1();
            Integer count = tuple._2();
            return new Tuple2<>(userTweetPair._1(), new Tuple2<>(userTweetPair._2(), count)); //(userId, (tweetId, count))
        });

        JavaPairRDD<Long, Iterable<Tuple2<Long, Integer>>> groupedByUserRDD = user_Tweet_MaxCount_PairRDD.groupByKey();

        // Build the following RDD: (userId, (maxTweetId, count))
        JavaPairRDD<Long, Tuple2<Long, Integer>> mostRetweetedTweets = groupedByUserRDD.mapValues(tweetCountIterable -> {
            Tuple2<Long, Integer> maxCountTuple = null;
            int maxCount = -1;
            for (Tuple2<Long, Integer> tweetCount : tweetCountIterable) {
                if (tweetCount._2() > maxCount) {
                    maxCount = tweetCount._2();
                    maxCountTuple = tweetCount;
                }
            }
            return maxCountTuple;
        });

        // Save the result as a text file
        mostRetweetedTweets.saveAsTextFile(output);
        System.out.println("Finding most retweeted tweets completed successfully");
        sc.stop();
    }
}