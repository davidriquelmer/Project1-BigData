import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        //yeah mostro
        java.lang.StringBuilder lista= new java.lang.StringBuilder();
        int c=0;
        for (Text value : values){
            lista.append(","+ value);
            c++;
        }
        context.write(key, new Text("["+c+"]"+lista.toString()));

    }
}
