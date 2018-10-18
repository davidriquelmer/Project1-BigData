import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;

public class mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();
        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            String tweet = status.getText().toUpperCase().replaceAll("[^A-Z ]", "");
            String[] stopWords = {"A","ABOUT","ABOVE","AFTER","AGAIN","AGAINST","ALL","AM","AN","AND","ANY","ARE","AREN'T","AS","AT","BE","BECAUSE","BEEN","BEFORE","BEING","BELOW","BETWEEN","BOTH","BUT","BY","CAN'T",
                                    "CANNOT","COULD","COULDN'T","DID","DIDN'T","DO","DOES","DOESN'T","DOING","DON'T","DOWN","DURING","EACH","FEW","FOR","FROM","FURTHER","HAD","HADN'T","HAS","HASN'T","HAVE","HAVEN'T",
                                    "HAVING","HE","HE'D","HE'LL","HE'S","HER","HERE","HERE'S","HERS","HERSELF","HIM","HIMSELF","HIS","HOW","HOW'S","I","I'D","I'LL","I'M","I'VE","IF","IN","INTO","IS","ISN'T","IT","IT'S",
                                    "ITS","ITSELF","LET'S","ME","MORE","MOST","MUSTN'T","MY","MYSELF","NO","NOR","NOT","OF","OFF","ON","ONCE","ONLY","OR","OTHER","OUGHT","OUR","OURS","OURSELVES","OUT","OVER","OWN","SAME",
                                    "SHAN'T","SHE","SHE'D","SHE'LL","SHE'S","SHOULD","SHOULDN'T","SO","SOME","SUCH","THAN","THAT","THAT'S","THE","THEIR","THEIRS","THEM","THEMSELVES","THEN","THERE","THERE'S","THESE","THEY",
                                    "THEY'D","THEY'LL","THEY'RE","THEY'VE","THIS","THOSE","THROUGH","TO","TOO","UNDER","UNTIL","UP","VERY","WAS","WASN'T","WE","WE'D","WE'LL","WE'RE","WE'VE","WERE","WEREN'T","WHAT","WHAT'S",
                                    "WHEN","WHEN'S","WHERE","WHERE'S","WHICH","WHILE","WHO","WHO'S","WHOM","WHY","WHY'S","WITH","WON'T","WOULD","WOULDN'T","YOU","YOU'D","YOU'LL","YOU'RE","YOU'VE","YOUR","YOURS","YOURSELF",
                                    "YOURSELVES","A","ABOUT","ABOVE","AFTER","AGAIN","AGAINST","ALL","AM","AN","AND","ANY","ARE","AREN'T","AS","AT","BE","BECAUSE","BEEN","BEFORE","BEING","BELOW","BETWEEN","BOTH","BUT","BY",
                                    "CAN'T","CANNOT","COULD","COULDN'T","DID","DIDN'T","DO","DOES","DOESN'T","DOING","DON'T","DOWN","DURING","EACH","FEW","FOR","FROM","FURTHER","HAD","HADN'T","HAS","HASN'T","HAVE","HAVEN'T",
                                    "HAVING","HE","HE'D","HE'LL","HE'S","HER","HERE","HERE'S","HERS","HERSELF","HIM","HIMSELF","HIS","HOW","HOW'S","I","I'D","I'LL","I'M","I'VE","IF","IN","INTO","IS","ISN'T","IT","IT'S","ITS",
                                    "ITSELF","LET'S","ME","MORE","MOST","MUSTN'T","MY","MYSELF","NO","NOR","NOT","OF","OFF","ON","ONCE","ONLY","OR","OTHER","OUGHT","OUR","OURS","OURSELVES","OUT","OVER","OWN","SAME","SHAN'T",
                                    "SHE","SHE'D","SHE'LL","SHE'S","SHOULD","SHOULDN'T","SO","SOME","SUCH","THAN","THAT","THAT'S","THE","THEIR","THEIRS","THEM","THEMSELVES","THEN","THERE","THERE'S","THESE","THEY","THEY'D",
                                    "THEY'LL","THEY'RE","THEY'VE","THIS","THOSE","THROUGH","TO","TOO","UNDER","UNTIL","UP","VERY","WAS","WASN'T","WE","WE'D","WE'LL","WE'RE","WE'VE","WERE","WEREN'T","WHAT","WHAT'S","WHEN","WHEN'S",
                                    "WHERE","WHERE'S","WHICH","WHILE","WHO","WHO'S","WHOM","WHY","WHY'S","WITH","WON'T","WOULD","WOULDN'T","YOU","YOU'D","YOU'LL","YOU'RE","YOU'VE","YOUR","YOURS","YOURSELF","YOURSELVES","A","ABOUT",
                                    "ABOVE","AFTER","AGAIN","AGAINST","ALL","AM","AN","AND","ANY","ARE","AREN'T","AS","AT","BE","BECAUSE","BEEN","BEFORE","BEING","BELOW","BETWEEN","BOTH","BUT","BY","CAN'T","CANNOT","COULD","COULDN'T",
                                    "DID","DIDN'T","DO","DOES","DOESN'T","DOING","DON'T","DOWN","DURING","EACH","FEW","FOR","FROM","FURTHER","HAD","HADN'T","HAS","HASN'T","HAVE","HAVEN'T","HAVING","HE","HE'D","HE'LL","HE'S","HER","HERE",
                                    "HERE'S","HERS","HERSELF","HIM","HIMSELF","HIS","HOW","HOW'S","I","I'D","I'LL","I'M","I'VE","IF","IN","INTO","IS","ISN'T","IT","IT'S","ITS","ITSELF","LET'S","ME","MORE","MOST","MUSTN'T","MY","MYSELF",
                                    "NO","NOR","NOT","OF","OFF","ON","ONCE","ONLY","OR","OTHER","OUGHT","OUR","OURS","OURSELVES","OUT","OVER","OWN","SAME","SHAN'T","SHE","SHE'D","SHE'LL","SHE'S","SHOULD","SHOULDN'T","SO","SOME","SUCH",
                                    "THAN","THAT","THAT'S","THE","THEIR","THEIRS","THEM","THEMSELVES","THEN","THERE","THERE'S","THESE","THEY","THEY'D","THEY'LL","THEY'RE","THEY'VE","THIS","THOSE","THROUGH","TO","TOO","UNDER","UNTIL","UP",
                                    "VERY","WAS","WASN'T","WE","WE'D","WE'LL","WE'RE","WE'VE","WERE","WEREN'T","WHAT","WHAT'S","WHEN","WHEN'S","WHERE","WHERE'S","WHICH","WHILE","WHO","WHO'S","WHOM","WHY","WHY'S","WITH","WON'T","WOULD",
                                    "WOULDN'T","YOU","YOU'D","YOU'LL","YOU'RE","YOU'VE","YOUR","YOURS","YOURSELF","YOURSELVES","A","ABOUT","ABOVE","AFTER","AGAIN","AGAINST","ALL","AM","AN","AND","ANY","ARE","AREN'T","AS","AT","BE","BECAUSE",
                                    "BEEN","BEFORE","BEING","BELOW","BETWEEN","BOTH","BUT","BY","CAN'T","CANNOT","COULD","COULDN'T","DID","DIDN'T","DO","DOES","DOESN'T","DOING","DON'T","DOWN","DURING","EACH","FEW","FOR","FROM","FURTHER","HAD",
                                    "HADN'T","HAS","HASN'T","HAVE","HAVEN'T","HAVING","HE","HE'D","HE'LL","HE'S","HER","HERE","HERE'S","HERS","HERSELF","HIM","HIMSELF","HIS","HOW","HOW'S","I","I'D","I'LL","I'M","I'VE","IF","IN","INTO","IS",
                                    "ISN'T","IT","IT'S","ITS","ITSELF","LET'S","ME","MORE","MOST","MUSTN'T","MY","MYSELF","NO","NOR","NOT","OF","OFF","ON","ONCE","ONLY","OR","OTHER","OUGHT","OUR","OURS","OURSELVES","OUT","OVER","OWN","SAME",
                                    "SHAN'T","SHE","SHE'D","SHE'LL","SHE'S","SHOULD","SHOULDN'T","SO","SOME","SUCH","THAN","THAT","THAT'S","THE","THEIR","THEIRS","THEM","THEMSELVES","THEN","THERE","THERE'S","THESE","THEY","THEY'D","THEY'LL",
                                    "THEY'RE","THEY'VE","THIS","THOSE","THROUGH","TO","TOO","UNDER","UNTIL","UP","VERY","WAS","WASN'T","WE","WE'D","WE'LL","WE'RE","WE'VE","WERE","WEREN'T","WHAT","WHAT'S","WHEN","WHEN'S","WHERE","WHERE'S",
                                    "WHICH","WHILE","WHO","WHO'S","WHOM","WHY","WHY'S","WITH","WON'T","WOULD","WOULDN'T","YOU","YOU'D","YOU'LL","YOU'RE","YOU'VE","YOUR","YOURS","YOURSELF","YOURSELVES"};
            String[] temp;
            temp = tweet.split(" ");
            for (String aTemp : temp) {
                if (aTemp.length() > 2) {
                    boolean same = false;
                    String word = "";
                    for (String stopWord : stopWords) {
                        //---- comprobar si es igual a StopWords y no lo agrego
                        if (stopWord.equalsIgnoreCase(aTemp)) {
                            same = true;
                        }
                        word = aTemp;
                    }
                    if (!same) {
                        context.write(new Text(word), new IntWritable(1));
                    }
                }

            }
        }catch(TwitterException e){
        }
    }
}
