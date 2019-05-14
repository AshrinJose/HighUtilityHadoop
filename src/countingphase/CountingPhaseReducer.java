
package countingphase;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Ashrin Rose Jose
 */
public class CountingPhaseReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override   
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;

        for (LongWritable value : values) {
            sum +=value.get();
        }

        context.write(key, new LongWritable(sum));
    }

}
