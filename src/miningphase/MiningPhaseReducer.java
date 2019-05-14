
package miningphase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import main.Home;
import main.JobRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Ashrin Rose Jose
 */
 public class MiningPhaseReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException 
    {
        Configuration conf = context.getConfiguration();
        long minUtility = JobRunner.minUtility;
        
        List<String> inputs = new ArrayList<>();
        int totalUtility = 0;
                
            for (Text value : values) 
            {    
                String condiUtrans = value.toString();
                int prefixUtility = Integer.parseInt(condiUtrans.split(",")[1].trim());
                totalUtility += prefixUtility;
                if (!inputs.contains(value.toString())) 
                {
                    inputs.add(value.toString());
                }                
            }
           
          if(totalUtility>=minUtility)
          {
              Map<String, Long> TWUMap = new HashMap<String, Long>();
              
              for (String condUtrn : inputs) 
              {

                long TU = Integer.parseInt(condUtrn.split(",")[1].trim());

                String[] items = condUtrn.split(",")[2].trim().split("\\s+");

                String[] utils = condUtrn.split(",")[3].trim().split("\\s+");

                for (int i = 0; i < items.length&&!items[i].equals(""); i++) 
                {
                    String item = items[i];
                    long LTWU = Long.parseLong(utils[i].trim()) + TU;

                    if (TWUMap.containsKey(item)) 
                    {
                        LTWU += TWUMap.get(item);
                    }
                    TWUMap.put(item, LTWU);
                }
            }
           

           
            /**
             * Pass two to discard unpromising items.
             */
            
            for (String condUtrn : inputs) 
            {
                String[] items = condUtrn.split(",")[2].trim().split("\\s+");
                String[] utils = condUtrn.split(",")[3].trim().split("\\s+");

                List<String> itemsField = new ArrayList<>();
                List<String> utilsField = new ArrayList<>();

                for (int i = 0; i < items.length&&!items[i].equals(""); i++) {

                    String item = items[i];
                    if (TWUMap.get(item) >= Home.minUtility2) {
                        itemsField.add(item);
                        utilsField.add(utils[i]);
                    }
                }

                if (!itemsField.isEmpty()) 
                {
                    
                    context.write(key,new Text(condUtrn));   
                }
            }
          }   
        }
}
