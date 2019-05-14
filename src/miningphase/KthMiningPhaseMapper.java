
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
public class KthMiningPhaseMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws
            IOException, InterruptedException {
        String k = "";
        String v = "";

         System.out.println("\n\n----------MAPPER-----------\n");
        String thisLine = value.toString().split("\t")[1];
       
                
        String prefix = thisLine.split(",")[0];
        long prefixUtility = Long.parseLong(thisLine.split(",")[1].trim());

        String[] items = thisLine.split(",")[2].trim().split("\\s+");
        String[] utils = thisLine.split(",")[3].trim().split("\\s+");
        
        
        for (int i = 0; i < items.length; i++) {

            String item = prefix + items[i];
            String util = String.valueOf(Long.parseLong(utils[i]) + prefixUtility);

            List<String> conUtrn = new ArrayList<String>();
            conUtrn.add(item);
            conUtrn.add(util);

            String[] itemsField = new String[items.length - i - 1];
            System.arraycopy(items, i + 1, itemsField, 0, itemsField.length);

            String[] utilsField = new String[utils.length - i - 1];
            System.arraycopy(utils, i + 1, utilsField, 0, utilsField.length);

            String itms = Arrays.toString(itemsField).replace("[", "").replace("]", "").replaceAll(",", "");
            String utls = Arrays.toString(utilsField).replace("[", "").replace("]", "").replaceAll(",", "");

            conUtrn.add(itms);
            conUtrn.add(utls);
            System.out.println("conUtrn = " + conUtrn);
            
            k=k.concat("<").concat(thisLine).concat(">");
            v=v.concat("<").concat(conUtrn.toString()).concat(">");

            context.write(new Text(k), new Text(conUtrn.toString().replace("[", "").replace("]", "")));
        

        }

    }
}
