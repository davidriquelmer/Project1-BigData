import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class reducer extends Reducer<Text, Text, Text, Text> {
    protected void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        java.lang.StringBuilder lista= new java.lang.StringBuilder();
        for (Text value : values){
            lista.append("," + value);
        }
        context.write(key, new Text(lista.toString()));
    }
}
