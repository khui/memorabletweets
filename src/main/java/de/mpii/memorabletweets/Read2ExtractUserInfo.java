package de.mpii.memorabletweets;

import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.zip.GZIPInputStream;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import twitter4j.User;

/**
 *
 * @author khui
 */
public class Read2ExtractUserInfo {

    static Logger logger = Logger.getLogger(Read2ExtractUserInfo.class.getName());

    private final TLongSet userIds = new TLongHashSet();

    public void extractGzip(String gzipdir, String outputf) throws IOException, TwitterException, InterruptedException {
        BufferedReader br;
        String jsonStr;
        Status tweet;
        File directory = new File(gzipdir);
        PrintStream ps = new PrintStream(outputf);
        int count = 0;
        for (File fileEntry : directory.listFiles()) {
            br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(fileEntry))));
            while (br.ready()) {
                jsonStr = br.readLine();
                tweet = TwitterObjectFactory.createStatus(jsonStr);
                User user = tweet.getUser();
                if (user != null) {
                    count++;
                    printuserinfo(ps, user);
                    if (count % 10000 == 0) {
                        logger.info("read in tweets: " + count);
                    }
                }
            }
            br.close();
        }
        ps.close();
    }

    private void printuserinfo(PrintStream ps, User user) {
        long userid = user.getId();
        String username = user.getName();
        int num_followers = user.getFollowersCount();
        int num_tweets = user.getStatusesCount();
        if (num_followers > 500 && num_tweets > 3500) {
            if (!userIds.contains(userid)) {
                StringBuilder sb = new StringBuilder();
                sb.append(userid).append("\t");
                sb.append(username).append("\t");
                sb.append(num_followers).append("\t");
                sb.append(num_tweets);
                ps.println(sb.toString());
                userIds.add(userid);
            }
        }
    }

    public static void main(String[] args) throws ParseException, IOException, TwitterException, InterruptedException {
        Options options = new Options();
        options.addOption("o", "outfile", true, "output file");
        options.addOption("i", "indexdirectory", true, "index directory");
        options.addOption("l", "log4jxml", true, "log4j conf file");
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);
        String outputfile = null, log4jconf = null, gzipdir = null;
        if (cmd.hasOption("o")) {
            outputfile = cmd.getOptionValue("o");
        }
        if (cmd.hasOption("l")) {
            log4jconf = cmd.getOptionValue("l");
        }

        if (cmd.hasOption("i")) {
            gzipdir = cmd.getOptionValue("i");
        }
        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
        LogManager.getRootLogger().setLevel(Level.INFO);
        logger.info("Read2ExtractUserInfo start.");

        Read2ExtractUserInfo reui = new Read2ExtractUserInfo();
        reui.extractGzip(gzipdir, outputfile);

    }

}
