
package countingphase;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Ashrin Rose Jose
 */
public class CountingPhaseMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws
            IOException, InterruptedException {

        System.out.println("\n\n----------MAPPER-----------\n");

        String thisLine = value.toString();
        String[] split = thisLine.split(":");

        String items[] = split[0].split(" ");
        // the second part is the transaction utility
        int trnsUtil = Integer.parseInt(split[1]);

        for (String item : items) {
            // get the current TWU of that item
            
            Text trn = new Text(item);
            LongWritable trnUtil = new LongWritable(trnsUtil);

            context.write(trn, trnUtil);
        }
     

    }
}
