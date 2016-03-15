package de.mpii.microblogtrack.task.archiver;

import gnu.trove.list.array.TLongArrayList;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.compress.archivers.ArchiveException;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.*;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

public final class LookupT4j {

    private int numInCurrentArchive = 0;

    private HashMap<String, Twitter> apikeyTwitter = new HashMap<String, Twitter>();

    private Map<String, Long> apikeyTimestamp = new HashMap<>();

    private Twitter currenttwitter;

    private String currentKey;

    private final String keydirectory;

    private final String expid = "expid";

    private long totalwaittime = 0;

    private final int numoffileinzip = 50000;

    public LookupT4j(String keydir){
        this.keydirectory = keydir;
    }

    private ResponseList<Status> getbatchtweets(long[] tweetids) throws InterruptedException, FileNotFoundException {
        ResponseList<Status> statuslist = null;
        checkratelimit();
        try {
            statuslist = currenttwitter.lookup(tweetids);
        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println(expid + " failed to show status: " + te.getMessage());
        }
        return statuslist;
    }

    private void checkratelimit() throws InterruptedException, FileNotFoundException {
        RateLimitStatus ratestatus;
        int second2wait, remain, limit;
        long currenttime = System.currentTimeMillis();
        long currentkeyTimestamp = apikeyTimestamp.get(currentKey);
        try {
            if (currenttime > currentkeyTimestamp) {
                ratestatus = currenttwitter.getRateLimitStatus().get(
                        "/statuses/lookup");
                limit = ratestatus.getLimit();
                second2wait = ratestatus.getSecondsUntilReset();
                remain = ratestatus.getRemaining();
                if (remain <= 1 && second2wait > 0) {
                    long nextsession = (second2wait * 1005) + currenttime;
                    apikeyTimestamp.put(currentKey, nextsession);
                    updateTwitter();
                    long waittime = apikeyTimestamp.get(currentKey) - currenttime + 5000;
                    if (waittime > 0) {
                        totalwaittime += waittime;
                        System.out.println(expid + " " + currentKey + ": " + limit
                                + " is exhasusted, waiting time: "
                                + (waittime / 1000) + " seconds.");
                        Thread.sleep(waittime);
                    }
                }
            } else {
                long waittime = currentkeyTimestamp - currenttime + 10000;
                totalwaittime += waittime;
                System.out.println(expid + " " + currentKey + ": waiting time: "
                        + (waittime / 1000) + " seconds.");
                Thread.sleep(waittime);
            }
        } catch (TwitterException te) {
            if (te.exceededRateLimitation()) {
                te.printStackTrace();
            }
        } finally {
            writeKeyTimestamp();
        }
    }

    public static long[] readintweetids(String file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(new File(file)));
        TLongArrayList tweetids = new TLongArrayList();
        while (br.ready()) {
            String line = br.readLine();
            long tweetid = Long.parseLong(line);
            tweetids.add(tweetid);
        }
        br.close();
        return tweetids.toArray();
    }

    private void addToArchive(String fileName, String rawJson,
                              ZipOutputStream currentArchive) throws IOException,
            ArchiveException {
        byte[] data = rawJson.getBytes();
        ZipEntry entry = new ZipEntry(fileName);
        entry.setSize(data.length);
        currentArchive.putNextEntry(entry);
        currentArchive.write(data);
        currentArchive.closeEntry();
    }

    private void writeKeyTimestamp() throws FileNotFoundException {
        PrintStream ps = new PrintStream(new File(keydirectory, "key-timestamp"));
        for (String apikey : apikeyTimestamp.keySet()) {
            ps.println(apikey + " " + apikeyTimestamp.get(apikey));
        }
        ps.close();
    }

    private void readinAPIKeys() throws IOException {
        BufferedReader br;
        ConfigurationBuilder cb;
        int keynum = 0;
        String oauthConsumerKey = null,
                oauthConsumerSecret = null,
                oauthAccessToken = null,
                oauthAccessTokenSecret = null;
        br = new BufferedReader(new FileReader(new File(keydirectory, "key-timestamp")));
        while (br.ready()) {
            String line = br.readLine();
            String[] cols = line.split(" ");
            if (cols.length == 2) {
                apikeyTimestamp.put(cols[0], Long.parseLong(cols[1]));
            }
        }
        br.close();
        for (String keyfile : new File(keydirectory).list()) {
            if (keyfile.equals("key-timestamp")) {
                continue;
            }
            br = new BufferedReader(new FileReader(new File(keydirectory, keyfile)));
            while (br.ready()) {
                String line = br.readLine();
                String[] cols = line.split("=");
                if (cols[0].equals("oauth.consumerKey")) {
                    oauthConsumerKey = cols[1];
                } else if (cols[0].equals("oauth.consumerSecret")) {
                    oauthConsumerSecret = cols[1];
                } else if (cols[0].equals("oauth.accessToken")) {
                    oauthAccessToken = cols[1];
                } else if (cols[0].equals("oauth.accessTokenSecret")) {
                    oauthAccessTokenSecret = cols[1];
                }
            }
            br.close();
            cb = new ConfigurationBuilder();
            cb.setJSONStoreEnabled(true)
                    .setOAuthConsumerKey(oauthConsumerKey)
                    .setOAuthConsumerSecret(oauthConsumerSecret)
                    .setOAuthAccessToken(oauthAccessToken)
                    .setOAuthAccessTokenSecret(oauthAccessTokenSecret);
            long timestamprecord = apikeyTimestamp.get(keyfile);
            long currenttimestamp = System.currentTimeMillis() + (keynum++);
            if (currenttimestamp < timestamprecord) {
                currenttimestamp = timestamprecord;
            }
            apikeyTwitter.put(keyfile, new TwitterFactory(cb.build()).getInstance());
            apikeyTimestamp.put(keyfile, currenttimestamp);
        }
        System.out.println(expid + " Established " + apikeyTwitter.size() + " connections.");
    }

    private void updateTwitter() {
        Long[] milsecond = apikeyTimestamp.values().toArray(new Long[0]);
        Arrays.sort(milsecond);
        long minimumTime = milsecond[0];
        for (String apikey : apikeyTwitter.keySet()) {
            if (apikeyTimestamp.get(apikey) <= minimumTime) {
                currenttwitter = apikeyTwitter.get(apikey);
                currentKey = apikey;
                break;
            }
        }
    }

    public void crawltweets(String tweetidfile, String outdirectory) throws IOException, InterruptedException, ArchiveException {
        int zipcount = 1;
        PrintStream pslog = new PrintStream(new File(outdirectory, expid + "-zipfnum.log"));
        ZipOutputStream currentArchiveStream = new ZipOutputStream(
                new BufferedOutputStream(new FileOutputStream(new File(
                        outdirectory, expid + "-" + (zipcount++) + ".zip"))));
        long[] tweetids = readintweetids(tweetidfile);
        TLongArrayList alltweetids = new TLongArrayList(tweetids);
        ResponseList<Status> tweetspack;
        int from = 0;
        int to = 0;
        int length = 100;
        readinAPIKeys();
        updateTwitter();
        while (true) {
            to = from + length;
            long[] requesttweets = Arrays.copyOfRange(tweetids, from, to);
            tweetspack = getbatchtweets(requesttweets);
            for (Status tweet : tweetspack) {
                long tweetid = tweet.getId();
                alltweetids.remove(tweetid);
                String rawJSON = TwitterObjectFactory.getRawJSON(tweet);
                addToArchive(tweetid + ".json", rawJSON, currentArchiveStream);
                numInCurrentArchive++;
                if (numInCurrentArchive >= numoffileinzip) {
                    currentArchiveStream.finish();
                    currentArchiveStream.flush();
                    currentArchiveStream.close();
                    pslog.println(expid + "-" + (zipcount - 1) + " : " + numInCurrentArchive);
                    numInCurrentArchive = 0;
                    currentArchiveStream = new ZipOutputStream(
                            new BufferedOutputStream(new FileOutputStream(
                                    new File(outdirectory, expid + "-" + (zipcount++)
                                            + ".zip"))));
                }
            }
            from = to;
            if (from % (18000 * apikeyTwitter.size()) == 0) {
                System.out.println(expid + " Finished crawling " + from + " out of " + tweetids.length);
            }
            if (from >= tweetids.length) {
                if (numInCurrentArchive > 0) {
                    currentArchiveStream.finish();
                    currentArchiveStream.flush();
                    currentArchiveStream.close();
                    pslog.println(expid + "-" + (zipcount - 1) + " : " + numInCurrentArchive);
                }
                break;
            }
        }
        if (alltweetids.size() > 0) {
            PrintStream ps = new PrintStream(new File(outdirectory, expid + "-misstweets.log"));
            for (long tweedid : alltweetids.toArray()) {
                ps.println(tweedid);
            }
            ps.close();
        }
        pslog.close();
        System.out.println(expid + " total waiting time: " + (totalwaittime / 1000) + " seconds.");
        writeKeyTimestamp();
    }

    public void crawltweets(long[] tweetids, String outdirectory) throws IOException, InterruptedException, ArchiveException {
        int zipcount = 1;
        PrintStream pslog = new PrintStream(new File(outdirectory, expid + "-zipfnum.log"));
        ZipOutputStream currentArchiveStream = new ZipOutputStream(
                new BufferedOutputStream(new FileOutputStream(new File(
                        outdirectory, expid + "-" + (zipcount++) + ".zip"))));
        TLongArrayList alltweetids = new TLongArrayList(tweetids);
        ResponseList<Status> tweetspack;
        int from = 0;
        int to = 0;
        int length = 100;
        readinAPIKeys();
        updateTwitter();
        while (true) {
            to = from + length;
            long[] requesttweets = Arrays.copyOfRange(tweetids, from, to);
            tweetspack = getbatchtweets(requesttweets);
            for (Status tweet : tweetspack) {
                long tweetid = tweet.getId();
                alltweetids.remove(tweetid);
                String rawJSON = TwitterObjectFactory.getRawJSON(tweet);
                addToArchive(tweetid + ".json", rawJSON, currentArchiveStream);
                numInCurrentArchive++;
                if (numInCurrentArchive >= numoffileinzip) {
                    currentArchiveStream.finish();
                    currentArchiveStream.flush();
                    currentArchiveStream.close();
                    pslog.println(expid + "-" + (zipcount - 1) + " : " + numInCurrentArchive);
                    numInCurrentArchive = 0;
                    currentArchiveStream = new ZipOutputStream(
                            new BufferedOutputStream(new FileOutputStream(
                                    new File(outdirectory, expid + "-" + (zipcount++)
                                            + ".zip"))));
                }
            }
            from = to;
            if (from % (18000 * apikeyTwitter.size()) == 0) {
                System.out.println(expid + " Finished crawling " + from + " out of " + tweetids.length);
            }
            if (from >= tweetids.length) {
                if (numInCurrentArchive > 0) {
                    currentArchiveStream.finish();
                    currentArchiveStream.flush();
                    currentArchiveStream.close();
                    pslog.println(expid + "-" + (zipcount - 1) + " : " + numInCurrentArchive);
                }
                break;
            }
        }
        if (alltweetids.size() > 0) {
            PrintStream ps = new PrintStream(new File(outdirectory, expid + "-misstweets.log"));
            for (long tweedid : alltweetids.toArray()) {
                ps.println(tweedid);
            }
            ps.close();
        }
        pslog.close();
        System.out.println(expid + " total waiting time: " + (totalwaittime / 1000) + " seconds.");
        writeKeyTimestamp();
    }

    public static void readindemo(String filename) throws FileNotFoundException, IOException, TwitterException {
        ZipFile zipf = new ZipFile(filename);
        Enumeration<? extends ZipEntry> entries = zipf.entries();
        BufferedReader br;
        StringBuilder sb;
        while (entries.hasMoreElements()) {
            ZipEntry ze = (ZipEntry) entries.nextElement();
            String name = ze.getName();
            System.out.println(name);
            br = new BufferedReader(
                    new InputStreamReader(zipf.getInputStream(ze)));
            sb = new StringBuilder();
            while (br.ready()) {
                sb.append(br.readLine());
            }
            String jsonstr = sb.toString();
            Status tweet = TwitterObjectFactory.createStatus(jsonstr);
            System.out.println(tweet.getCreatedAt());
            System.out.println(tweet.getId());
            System.out.println(tweet.getLang());
            System.out.println(tweet.getText());
            System.out.println(tweet.getUser());
        }
        zipf.close();
    }

    public static void main(String[] args) throws ParseException, IOException, InterruptedException, ArchiveException, TwitterException, org.apache.commons.cli.ParseException {
        Options options = new Options();
        options.addOption("i", "tweetidinputf", true, "tweedids input file");
        options.addOption("o", "outdir", true, "output directory");
        options.addOption("n", "dataname", true, "tweetids data name");
        options.addOption("z", "filenuminzip", true, "file numbers per zip");
        options.addOption("k", "keyfile", true, "api key information");
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);
        String tweetidinputf = null, outputdirectory = null, keydirectory=null, expid;
        int numoffileinzip;
        if (cmd.hasOption("i")) {
            tweetidinputf = cmd.getOptionValue("i");
        }
        if (cmd.hasOption("o")) {
            outputdirectory = cmd.getOptionValue("o");
        }
        if (cmd.hasOption("k")) {
            keydirectory = cmd.getOptionValue("k");
        }
        if (cmd.hasOption("n")) {
            expid = cmd.getOptionValue("n");
        }
        if (cmd.hasOption("z")) {
            numoffileinzip = Integer.parseInt(cmd.getOptionValue("z"));
        }
        LookupT4j dtweet = new LookupT4j(keydirectory);
        dtweet.crawltweets(tweetidinputf, outputdirectory);
        //readindemo("/home/khui/workspace/result/data/microblog/tweet2011-1.zip");

    }
}