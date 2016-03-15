package de.mpii.memorabletweets;

import de.mpii.microblogtrack.task.archiver.LookupT4j;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import me.jhenrique.manager.TweetManager;
import me.jhenrique.manager.TwitterCriteria;
import me.jhenrique.model.Tweet;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import twitter4j.TwitterException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Created by khui on 09/03/16.
 */
public class HashtagUserTweetsJH {

    static Logger logger = Logger.getLogger(HashtagUserTweetsJH.class);
    private final String outdir;
    private final String since;
    private final String until;


    public HashtagUserTweetsJH(String outdir, String since, String until){
        this.outdir = outdir;
        this.since = since;
        this.until = until;
        if(!new File(outdir + "/tweetids").exists()){
            new File(outdir + "/tweetids").mkdir();
        }
    }

    private void write2file(String filename, List<Long> tweetids) throws IOException {
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(new File(outdir + "/tweetids", filename), true), StandardCharsets.UTF_8));
        for(long tweetid : tweetids){
            writer.write(String.valueOf(tweetid) + "\n");
        }
        writer.close();
    }

    private long[] readTweetids(String tweetidDir) throws IOException {
        File idDir = new File(tweetidDir);
        TLongSet tweetids = new TLongHashSet();
        if (idDir.isDirectory()){
            for (String f : idDir.list()){
                long[] tweetidInFile = LookupT4j.readintweetids(tweetidDir + "/" + f);
                for (long id : tweetidInFile){
                    tweetids.add(id);
                }
            }
        }
        logger.info("Read in " + tweetids.size() + " tweetids for processing.");
        return tweetids.toArray();
    }

    public void crawlTweetids(String keydir) throws IOException, InterruptedException, ArchiveException, TwitterException {
        long[] tweetids = readTweetids(outdir + "/tweetids");
        if (!new File(outdir + "/tweetjson").exists()) {
            new File(outdir + "/tweetjson").mkdir();
        } else {
            TLongSet tweetidExisted = Read2ExtractUserInfo.extractZip2TweetIds(outdir + "/tweetjson");
            TLongList tweetid2download = new TLongArrayList();
            for (long tweetid : tweetids) {
                if (!tweetidExisted.contains(tweetid)) {
                    tweetid2download.add(tweetid);
                }
            }
            tweetids = tweetid2download.toArray();
        }
        LookupT4j dtweet = new LookupT4j(keydir);
        logger.info("Remaining " + tweetids.length + " tweets to download.");
        dtweet.crawltweets(tweetids, outdir + "/tweetjson");
    }


    public void userTweetids(Set<String> users, String query) throws IOException {
        int count = 0;
        List<Long> tweetids = new ArrayList<>();
        TwitterCriteria criteria = TwitterCriteria.create().setSince(since).setUntil(until);
        for (String u : users){
            criteria.setUsername(u);
            List<Tweet> tweets = TweetManager.getTweets(criteria);
            for(Tweet t : tweets){
                long tweetid = Long.parseLong(t.getId());
                tweetids.add(tweetid);
                count++;
            }
            write2file(u + "_" + query, tweetids);
            logger.info("Tweets " + tweetids.size() + " for " +  u + " about " + query +", " + tweetids.size());
            tweetids.clear();
        }
        logger.info("Finished for query " + query + " on " + users.size() + " users with  " + count + " tweets.");
    }


    private Set<String> tweets2Users(String querystr) {
        List<Tweet> tweets;
        Set<String> users = new HashSet<>();
        try {
            TwitterCriteria criteria = TwitterCriteria.create().setQuerySearch(querystr);
            tweets = TweetManager.getTweets(criteria);
            logger.info("Get " + tweets.size() + " tweets in total for " + querystr);
            for (Tweet t : tweets) {
                users.add(t.getUsername());
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Failed to search users: " + e.getMessage());
            System.exit(-1);
        }
        logger.info("Get " + users.size() + " users for " + querystr);
        return users;
    }


    public static void  main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("l", "log4jxml", true, "log4jxml");
        options.addOption("s", "since", true, "since");
        options.addOption("u", "until", true, "until");
        options.addOption("o", "outdir", true, "output directory");
        options.addOption("q", "queries", true, "queries delimited by commas");
        options.addOption("k", "keydir", true, "token directory for tweet dumper");
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);
        String outdir = "", keydir = "", querystr = "#cikm,#sigir";
        String since="2015-03-01", until="2016-03-01";
        String log4jconf = "src/main/java/log4j.xml";
        if (cmd.hasOption("o")) {
            outdir = cmd.getOptionValue("o");
        }
        if (cmd.hasOption("q")) {
            querystr = cmd.getOptionValue("q");
        }
        if (cmd.hasOption("k")) {
            keydir = cmd.getOptionValue("k");
        }
        if (cmd.hasOption("s")) {
            since = cmd.getOptionValue("s");
        }
        if (cmd.hasOption("u")) {
            until = cmd.getOptionValue("u");
        }
        if (cmd.hasOption("l")) {
            log4jconf = cmd.getOptionValue("l");
        }
        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
        LogManager.getRootLogger().setLevel(Level.INFO);
        LogManager.getLogger("org.apache.http").setLevel(Level.OFF);
        HashtagUserTweetsJH hutjh = new HashtagUserTweetsJH(outdir, since, until);
        String[] queries = querystr.split(",");
        logger.info("Input queries " + queries.length + " from " + querystr);
        for (String query : queries) {
            if (query.length() < 2){
                continue;
            }
            Set<String> users = hutjh.tweets2Users(query);
            String expid = query+ "_" + since + "_" + until;
            hutjh.userTweetids(users, expid);
        }
        hutjh.crawlTweetids(keydir);
    }
}
