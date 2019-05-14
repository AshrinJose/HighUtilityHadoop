
package dbtransformationphase;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Ashrin Rose Jose
 */
public class TransformatinPhase {

    public static long execute(String[] jobSet2, long minUtility) throws IOException {

        List<String> TWUs = new ArrayList<>();
        Map<String, Long> TWUMap = new HashMap<>();

        File f = new File(jobSet2[1]);

        BufferedReader br = new BufferedReader(new FileReader(f));
        String line;
        long t1 = System.currentTimeMillis();
        while ((line = br.readLine()) != null) {

            String[] split = line.split("\\s+");
            String item = split[0];
            long utilVal = Long.parseLong(split[1].trim());
            if (utilVal >= minUtility) {
                TWUMap.put(item, utilVal);
            }
        }
        br.close();
        TWUMap = sortByValues(TWUMap);

        System.out.println("TWU = " + TWUMap);

        br = new BufferedReader(new FileReader(jobSet2[0]));
        BufferedWriter bw = new BufferedWriter(new FileWriter(jobSet2[2]));

        while ((line = br.readLine()) != null) {
            String[] items = line.split(":")[0].split("\\s+");
            String[] utils = line.split(":")[2].split("\\s+");
           
            Map<String, Long> tempMap = new HashMap<String, Long>();

            for (int i = 0; i < items.length; i++) {
                String item = items[i];
                if (TWUMap.containsKey(item)) {
                    tempMap.put(item, Long.parseLong(utils[i]));
                }
            }
            tempMap = sort(tempMap,TWUMap);
            Collection<Long> values = tempMap.values();
            long totalUtil = 0;
            for (Long value : values) {
                totalUtil += value;
            }

            String keySet = tempMap.keySet().toString().replace("[", "").replace("]", "").replace(",", "");
            keySet = keySet.concat(" :").concat(String.valueOf(totalUtil)).concat(": ");

            String valueSet = tempMap.values().toString().replace("[", "").replace("]", "").replace(",", "");
            keySet = keySet.concat(valueSet).concat("\n");

            bw.write(keySet);

        }
        bw.close();
        br.close();
        long t2 = System.currentTimeMillis();
        return (t2 - t1);
    }

    private static Map sortByValues(Map<String, Long> map) {

        List list = new LinkedList(map.entrySet());
        // Defined Custom Comparator here
        Collections.sort(list, new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                return ((Comparable) ((Map.Entry) (o1)).getValue())
                        .compareTo(((Map.Entry) (o2)).getValue());
            }
        });

        // Here I am copying the sorted list in HashMap
        // using LinkedHashMap to preserve the insertion order
        HashMap sortedHashMap = new LinkedHashMap();
        for (Iterator it = list.iterator(); it.hasNext();) {
            Map.Entry entry = (Map.Entry) it.next();
            sortedHashMap.put(entry.getKey(), entry.getValue());
        }
        return sortedHashMap;
    }

    private static Map<String, Long> sort(Map<String, Long> tempMap, Map<String, Long> TWUMap) {
        Map <String, Long> sortedMap = new LinkedHashMap<>();
        for (String key : TWUMap.keySet()) {
            if(tempMap.containsKey(key)){
                sortedMap.put(key, tempMap.get(key));
            }
        }
        return sortedMap;
        
    }

}
