import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

public class mapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String rawTweet = value.toString();
        try{
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            if (status.isRetweet()) {
                //identify id
                long tweetID = status.getRetweetedStatus().getId();
                context.write(new Text(String.valueOf(tweetID)),new Text(Long.toString(status.getId())));
            }
        }catch (TwitterException e) {}
    }
}
