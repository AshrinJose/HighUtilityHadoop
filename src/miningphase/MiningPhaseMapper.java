
package miningphase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Ashrin Rose Jose
 */
public class MiningPhaseMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws
            IOException, InterruptedException {

        System.out.println("\n\n----------MAPPER-----------\n");

        
        String line = value.toString();
            String[] items = line.split(":")[0].trim().split("\\s+");
            String[] utils = line.split(":")[2].trim().split("\\s+");
            for (int i = 0; i < items.length; i++) {
                String item = items[i];
                String util = utils[i];

                List<String> arrList = new ArrayList<>();
                arrList.add(item);
                arrList.add(util);

                String[] itemsField = new String[items.length - i - 1];
                System.arraycopy(items, i + 1, itemsField, 0, itemsField.length);

                String[] utilsField = new String[utils.length - i - 1];
                System.arraycopy(utils, i + 1, utilsField, 0, utilsField.length);

                String itms = Arrays.toString(itemsField).replace("[", "").replace("]", "").replaceAll(",", "");
                String utls = Arrays.toString(utilsField).replace("[", "").replace("]", "").replaceAll(",", "");

                arrList.add(itms);
                arrList.add(utls);

                System.out.println("arry list= " + arrList);
                context.write(new Text(item), new Text(arrList.toString().replace("[", "").replace("]", "")));
        }
        
        
    }
}
