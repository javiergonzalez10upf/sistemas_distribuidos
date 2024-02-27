package edu.upf.model;

import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

import java.io.Serializable;
import java.util.Optional;

public class ExtendedSimplifiedTweet implements Serializable {
    private final long tweetId;
    private final String text;
    private final long userId;
// the id of the tweet (’id’)
    // the content of the tweet (’text’)
    // the user id (’user->id’)

    private final String userName; // the user name (’user’->’name’)
    private final long followersCount; // the number of followers (’user’->’followers_count’)
    private final String language; // the language of a tweet (’lang’)
    private final boolean isRetweeted; // is it a retweet? (the object ’retweeted_status’ exists?)
    private final Long retweetedUserId; // [if retweeted] (’retweeted_status’->’user’->’id’)
    private final Long retweetedTweetId; // [if retweeted] (’retweeted_status’->’id’)
    private final long timestampMs; // seconds from epoch (’timestamp_ms’)
    public ExtendedSimplifiedTweet(long tweetId, String text, long userId, String userName,
                                   long followersCount, String language, boolean isRetweeted,
                                   Long retweetedUserId, Long retweetedTweetId, long timestampMs) {
        this.tweetId = tweetId;
        this.text = text;
        this.userId = userId;
        this.userName = userName;
        this.followersCount = followersCount;
        this.language = language;
        this.isRetweeted = isRetweeted;
        this.retweetedUserId = retweetedUserId;
        this.retweetedTweetId = retweetedTweetId;
        this.timestampMs = timestampMs;
    }
    // IMPLEMENT ME
    /**
     * Returns a {@link ExtendedSimplifiedTweet} from a JSON String.
     * If parsing fails, for any reason, return an {@link Optional#empty()}
     *
     * @param jsonStr
     * @return an {@link Optional} of a {@link ExtendedSimplifiedTweet}
     */
    public static Optional<ExtendedSimplifiedTweet> fromJson(String jsonStr) {
        try {
            JsonParser jsonParser = new JsonParser();
            JsonObject jo = (JsonObject) jsonParser.parse(jsonStr);

            try {
                long tweetId = jo.get("id").getAsLong();
                String text = jo.get("text").getAsString();
                long userId = jo.getAsJsonObject("user").get("id").getAsLong();
                String userName = jo.getAsJsonObject("user").get("name").getAsString();
                long followersCount = jo.getAsJsonObject("user").get("followers_count").getAsLong();
                String language = jo.get("lang").getAsString();
                boolean isRetweeted = jo.has("retweeted_status");
                Long retweetedUserId = isRetweeted ? jo.getAsJsonObject("retweeted_status").getAsJsonObject("user").get("id").getAsLong() : null;
                Long retweetedTweetId = isRetweeted ? jo.getAsJsonObject("retweeted_status").get("id").getAsLong() : null;
                long timestampMs = jo.get("timestamp_ms").getAsLong();

                return Optional.of(new ExtendedSimplifiedTweet(tweetId, text, userId, userName, followersCount,
                        language, isRetweeted, retweetedUserId, retweetedTweetId, timestampMs));
            } catch (JsonParseException | IllegalStateException | NullPointerException e) {
                // Handle parsing errors
                return Optional.empty();
            }
        } catch (JsonParseException | IllegalStateException | NullPointerException e) {
            return Optional.empty();
        }
    }
    @Override
    public String toString() {
        return "ExtendedSimplifiedTweet{" +
                "tweetId=" + tweetId +
                ", text='" + text + '\'' +
                ", userId=" + userId +
                ", userName='" + userName + '\'' +
                ", followersCount=" + followersCount +
                ", language='" + language + '\'' +
                ", isRetweeted=" + isRetweeted +
                ", retweetedUserId=" + retweetedUserId +
                ", retweetedTweetId=" + retweetedTweetId +
                ", timestampMs=" + timestampMs +
                '}';
    }

    public boolean isRetweeted() {
        return isRetweeted;
    }

    public Long getRetweetedTweetId() {
        return retweetedTweetId;
    }

    public Long getRetweetedUserId() {
        return retweetedUserId;
    }

    public long getTweetId() {
        return tweetId;
    }

    public long getUserId() {
        return userId;
    }

    public String getLanguage() {
        return this.language;
    }
    public String getText() {return this.text;}
}


