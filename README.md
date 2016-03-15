This is a toolkit to extract tweets for a group of users who are interested in specific topic, e.g., information retrieval.
The toolkit mines all users whose tweets contain a given query, e.g., "#cikm". Afterward, the tweetids from the timelines for
these users are dumped to specific directory. Finally, using the tweet lookup api, we download and store the corresponding
tweet content for tweetids in json format, which can be digested by Twitter4j directly.

In favor of readability for human, the json file of tweets are stored in zip directory, which is less efficient in IO comparing
with gzip. When the number of tweet is huge, the io function should be rewritten to use gzip and save each tweet in a newline.
Another repository of mine, the microblogtrack, contains the method under the utility.io package. Given that sometimes the
twitter lookup api is not stable, the resumption after broken download is also implemented. The zip file that can not be re-open
properly will be deleted.

Usage:
de.mpii.memorabletweets.HashtagUserTweetsJH is the main entry.
And the keydir, outdir, queries, since, until, logconffile string can be passed thru command line, where 
-keydir indicates the directory contains the key for twitter api, i.e., customer key, customer secret, access token and the access token secret.
-outdir is the output directory, under which, $outdir/tweetids and $outdir/tweetjson are automatically created to store the 
tweetids and json files respectively.
-since, until: used in calling GetOldTweets (https://github.com/Jefferson-Henrique/GetOldTweets-java.git) toolkit, indicating
the time range of the tweets for a given user.
-logconffile is the configuration file for org.apache.log4j. An example for that is included in src/main/java/log4j.xml.
-queries is a string delimited by commas, each of the token is a query to create the initial user list



One example usage is as follows. Note that, to use the toolkit, you first need configure maven dependency management tool (https://maven.apache.org/). 
Depending on how you want to run the program, the hadoop jar can be replaced.

hadoop jar memorabletweets.jar de.mpii.memorabletweets.HashtagUserTweetsJH -o $outdir -k @keydir -s "2015-03-01"
                -u "2016-03-01" -l $logconffile -q "#cikm,#sigir"
