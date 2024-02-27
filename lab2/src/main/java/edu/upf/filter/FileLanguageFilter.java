package edu.upf.filter;

import edu.upf.model.SimplifiedTweet;
import org.apache.spark.api.java.JavaRDD;

import java.util.*;

public class FileLanguageFilter {
    public static JavaRDD<String> filterLanguage(JavaRDD<String> inputRDD, String language) {
        return inputRDD.filter(line -> {
            Optional<SimplifiedTweet> optionalTweet = SimplifiedTweet.fromJson(line);
            return optionalTweet.isPresent() && optionalTweet.get().getLanguage().equalsIgnoreCase(language);
        });
    }
}

