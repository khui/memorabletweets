package de.mpii.memorabletweets;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import twitter4j.*;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;


import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.nio.file.*;
import java.util.*;

/**
 * Created by khui on 09/03/16.
 */
public class HashtagUserTweetsT4J {

    static Logger logger = Logger.getLogger(HashtagUserTweetsT4J.class);


    public final Twitter twitter;

    public HashtagUserTweetsT4J(String consumerKey, String consumerSecret, String token, String secret){
        Configuration conf = configAccount(consumerKey, consumerSecret, token, secret);
        System.out.println(conf.isIncludeEntitiesEnabled());
        twitter = new TwitterFactory(conf).getInstance();

    }

    public void printUsers(List<String> queries, String outfile, int numoftoptweet) throws IOException{
        PrintStream ps = new PrintStream(outfile);
        for(String query : queries) {
            List<Status> tweets = searchTweets(query, numoftoptweet);
            logger.info("Retrieved " + tweets.size() + " tweets for " + query);
            List<User> users = tweets2Users(tweets);
            for (User user : users) {
                String userinfo = "@" + user.getScreenName() + "\t" + user.getId() + "\t" + user.getURL();
                ps.println(userinfo);
                logger.info(userinfo);
            }
        }
        ps.close();
    }

    public void printUserTweets(List<String> queries, String outdir, int numoftoptweet) throws IOException, ArchiveException, TwitterException{
        Map<Long, String> tweetidRawJson = new HashMap<>();
        for(String query : queries) {
            List<Status> tweets = searchTweets(query, numoftoptweet);
            List<User> users = tweets2Users(tweets);
            for (User user : users) {
                String userfilename = String.valueOf(user.getId());
                tweetidRawJson.clear();
                List<Status> userTweets = userTimeline(user.getId());
                for(Status tweet : userTweets){
                    String rawJSON = TwitterObjectFactory.getRawJSON(tweet);
                    if (rawJSON != null){
                        tweetidRawJson.put(tweet.getId(), rawJSON);
                    }
                }
                writeToZip(outdir + "/" + userfilename + "_" + tweetidRawJson.size(), tweetidRawJson);
                logger.info("Finished writing to " + userfilename);
            }
        }
    }

    private void writeToZip(String filename, Map<Long, String> tweetidRawJson)
            throws IOException, ArchiveException {
        Path root, zipfile;
        Map<String, String> zipproperties = new HashMap<>();
        zipproperties.put("create", "true");
        String zipfulluri = new File(filename).toString();
        URI zip_disk = URI.create("jar:file:" + zipfulluri);
        try (FileSystem zipfs = FileSystems.newFileSystem(zip_disk, zipproperties)) {
            for (long tweetid : tweetidRawJson.keySet()) {
                root = zipfs.getPath("/");
                zipfile = zipfs.getPath(root.toString(), String.valueOf(tweetid) + ".json");
                if (!Files.exists(zipfile)) {
                    Files.write(zipfile, tweetidRawJson.get(tweetid).getBytes(),
                            StandardOpenOption.CREATE,
                            StandardOpenOption.TRUNCATE_EXISTING);
                } else {
                    logger.error(tweetid + " already exists in " + filename);
                }
            }
        }
    }


    /**
     * at most we can fetch 3.2k tweets for a given user
     * @param userid
     * @throws TwitterException
     */
    private List<Status> userTimeline(long userid) throws TwitterException {
        Paging paging = new Paging();
        paging.setCount(200);
        List<Status> alltweets = new ArrayList<>();
        long lastID = Long.MAX_VALUE;
        while (true) {
            try {
                List<Status> tweetinonepage = twitter.getUserTimeline(userid, paging);
                if (tweetinonepage.isEmpty()){
                    break;
                }
                for (Status t: tweetinonepage){
                    if(t.getId() < lastID) lastID = t.getId();
                    alltweets.add(t);
                }
                paging.setMaxId(lastID-1);
            } catch (TwitterException te) {
                logger.error("userTimeline TwitterException: " + te);
            }
        }
        logger.info("Retrieved " + alltweets.size() + " tweets in total for " + userid);
        return alltweets;
    }

    private List<Status> searchTweets(String querystr, int numberOfTweets) {
        List<Status> tweets = new ArrayList<>();
        Query query = new Query(querystr);
        query.setLang("en");
        try {
            long lastID = Long.MAX_VALUE;
            int requestcount = 1;
            while (tweets.size () < numberOfTweets) {
                int singlefetchnum = Math.min(numberOfTweets, 100);
                if (numberOfTweets - tweets.size() > singlefetchnum)
                    query.setCount(singlefetchnum);
                else
                    query.setCount(singlefetchnum - tweets.size());
                try {
                    System.out.println(query.toString());
                    QueryResult result = twitter.search(query);
                    List<Status> resTweets = result.getTweets();
                    int resultcount = resTweets.size();
                    logger.info(result.toString() + "\t" + resultcount);
                    if (resultcount == 0){
                        break;
                    }
                    Thread.sleep(10000);

                    requestcount++;
                    if (requestcount >= 180) {
                        logger.warn("Rate limit reached and wait: " + querystr);
                        Thread.sleep(1000 * 60 * 15);
                        requestcount = 0;
                    }
                    tweets.addAll(resTweets);
                    logger.info("Fetched " + tweets.size() + " tweets");
                    for (Status t: tweets)
                        if(t.getId() < lastID) lastID = t.getId();

                }
                catch (TwitterException te) {
                    logger.info("Couldn't connect: " + te);
                }
                query.setMaxId(lastID-1);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Failed to search users: " + e.getMessage());
            System.exit(-1);
        }
        logger.info("Get " + tweets.size() + " tweets in total for " + querystr);
        return tweets;
    }

    private List<User> tweets2Users(List<Status> tweets){
        List<User> users = new ArrayList<>();
        Set<Long> userIds = new HashSet<>();
        for(Status tweet : tweets){
            long userid = tweet.getUser().getId();
            if (!userIds.contains(userid)){
                userIds.add(userid);
                users.add(tweet.getUser());
            }
        }
        return users;
    }



    private Configuration configAccount(String consumerKey, String consumerSecret, String token, String secret){
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setOAuthAccessToken(token);
        cb.setOAuthAccessTokenSecret(secret);
        cb.setOAuthConsumerKey(consumerKey);
        cb.setOAuthConsumerSecret(consumerSecret);
       // cb.setJSONStoreEnabled(true);
        cb.setIncludeEntitiesEnabled(false);
        System.out.println(cb);
        return cb.build();
    }

    public static void  main(String[] args) throws Exception{
        String log4jconf = "src/main/java/log4j.xml";
        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
        LogManager.getRootLogger().setLevel(Level.INFO);
        String consumerKey="ke3NjI5CiyCmP8PZwsN0nufmK";
        String consumerSecret="IBc1wddFQtcHvdRT8aJL4F8QFdU0E7ubkhg9AXsBvbe1RvhFTS";
        String token="335038158-55IbstIgPy4Igy4B5ZBWCrbISoAh5adX05hLAdeM";
        String secret="2fkymsHMOgb0nKo5Xb0e8uLQbKuK6dL6ZEoIYVv9jPFW8";
        String outdir = "/home/khui/workspace/result/memorabletweets";
        HashtagUserTweetsT4J hut = new HashtagUserTweetsT4J(consumerKey, consumerSecret, token, secret);
        QueryResult results = hut.twitter.search(new Query("#CIKM"));
        System.out.println(results.getRateLimitStatus());
        System.out.println(results.toString());
        for(Status tweet : results.getTweets()){
            System.out.println(tweet.toString());
        }
        hut.printUserTweets(Collections.singletonList("#CIKM"), outdir, 100);
    }
}
