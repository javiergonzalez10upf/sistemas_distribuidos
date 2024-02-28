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
                .groupBy(ExtendedSimplifiedTweet::getRetweetedUserId)
                .filter(tuple -> topUserIds.contains(tuple._1()));

        // ((UserX, TweetY), CountY)
        JavaRDD<Tuple2<Tuple2<Long, Long>, Integer>> userTweetCounts = tweetsGroupedByUser.flatMap(tuple -> {
            Long userId = tuple._1();
            Iterable<ExtendedSimplifiedTweet> tweets = tuple._2();

            List<Tuple2<Tuple2<Long, Long>, Integer>> userTweetCountList = new ArrayList<>();

            // Iterate over each tweet for the current user
            for (ExtendedSimplifiedTweet tweet : tweets) {
                Long tweetId = tweet.getTweetId();
                int count = 1; // Initialize count to 1 for the current tweet
                Tuple2<Long, Long> userTweetPair = new Tuple2<>(userId, tweetId);
                Tuple2<Tuple2<Long, Long>, Integer> userTweetCount = new Tuple2<>(userTweetPair, count);
                userTweetCountList.add(userTweetCount);
            }
            return userTweetCountList.iterator();
        });
        //(userId,tweet), count)

        //convert userTweetCounts to a JavaPairRDD with ((userId, tweetId), count)
        JavaPairRDD<Tuple2<Long, Long>, Integer> userTweetCountsPairRDD = userTweetCounts.mapToPair(tuple -> new Tuple2<>(tuple._1(), tuple._2()));

        //use reduceByKey to sum counts for each (user, tweet) pair
        JavaPairRDD<Tuple2<Long, Long>, Integer> aggregatedCounts = userTweetCountsPairRDD.reduceByKey(Integer::sum);

        JavaPairRDD<Long, Tuple2<Long, Integer>> userTweetMaxCountPairRDD = aggregatedCounts.mapToPair(tuple -> {
            Tuple2<Long, Long> userTweetPair = tuple._1();
            Integer count = tuple._2();
            return new Tuple2<>(userTweetPair._1(), new Tuple2<>(userTweetPair._2(), count));       //(userId, (tweetId, count))
        });

        JavaPairRDD<Long, Iterable<Tuple2<Long, Integer>>> groupedByUserRDD = userTweetMaxCountPairRDD.groupByKey();

        //for each user id, find the tuple with the maximum count value --> (userId, (maxTweetId, count))
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

        //guardem resultat com a fitxer
        mostRetweetedTweets.saveAsTextFile(output);
        System.out.println("Finding most retweeted tweets completed successfully");
    }
    /*
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
                    long userId = tweet.getRetweetedUserId();
                    return topUsersRetweeted.stream().anyMatch(tuple -> tuple._2() == userId);
                });
        topTweets.foreach(tweet -> System.out.println("Tweet: " + tweet));
        sc.stop();


        // Extract most retweeted tweets for each user
        //JavaRDD<ExtendedSimplifiedTweet> mostRetweetedTweets = mostRetweetedUsers
        //        .flatMap(userIdTweetId -> retweetsRDD
        //                .filter(tweet -> tweet.getUserId() == userIdTweetId && tweet.getTweetId() == mostRetweetedTweetId)
        //        );

        // Save the results
        topTweets.saveAsTextFile(output);
        topTweets.foreach(tweet -> System.out.println("Tweet: " + tweet));
        sc.stop();

    }*/

}