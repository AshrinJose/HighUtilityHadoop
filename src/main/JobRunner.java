
package main;

import countingphase.CountingPhaseMapper;
import countingphase.CountingPhaseReducer;
import miningphase.KthMiningPhaseMapper;
import miningphase.KthMiningPhaseReducer;
import miningphase.MiningPhaseMapper;
import miningphase.MiningPhaseReducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public final class JobRunner extends Configured implements Tool {

    static String hdfsFileInit = "hdfs://localhost:";
    static public long minUtility;
    static public int K;
    public static long totalTime = 0;

    /**
     * Run method which initializes the MapReduce job configuration and then
     * runs the corresponding job.
     *
     * @param args arguments passed from JobController
     * @return
     * @throws java.lang.Exception
     */
    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        conf.addResource("/etc/hadoop/conf−local/hdfs−site .xml");
        conf.addResource("/etc/hadoop/conf−local/mapred−site .xml");

        switch (args[0]) {
            case "Phase1":

                phaseOne(conf, args);
                break;
            case "Phase3":

                phaseThree(conf, args);
                break;

        }

        return (int) totalTime;
    }

    private void phaseOne(Configuration conf, String[] paths)
            throws Exception {

        hdfsFileInit = hdfsFileInit.concat(paths[3]);

        conf.set("fs.defaultFS", hdfsFileInit);

        hdfsFileInit = hdfsFileInit.concat(paths[4]);

        Job job = Job.getInstance();
        job.setJarByClass(JobRunner.class);
        job.setJobName("CountingPhase");

        job.setMapperClass(CountingPhaseMapper.class);
        job.setCombinerClass(CountingPhaseReducer.class);
        job.setReducerClass(CountingPhaseReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileSystem fs = FileSystem.get(conf);

        String[] hdfsPaths = {hdfsFileInit + "phase1input/", hdfsFileInit + "phase1output/"};

        /**
         * Creates the input directory, if it doesnt exist.
         */
        if (!fs.exists(new Path(hdfsPaths[0]))) {
            fs.mkdirs(new Path(hdfsPaths[0]));
        }
        /**
         * Deletes the output folder, if it exists.
         */
        if (fs.exists(new Path(hdfsPaths[1]))) {
            fs.delete(new Path(hdfsPaths[1]), true);
        }
        /**
         * Copy the dataset from the local folder to the HDFS file system.
         */
        fs.copyFromLocalFile(false, true, new Path(paths[1]), new Path(hdfsPaths[0]));

        TextInputFormat.setInputPaths(job, hdfsPaths[0] + new Path(paths[1]).getName());
        TextOutputFormat.setOutputPath(job, new Path(hdfsPaths[1]));

        long t1 = System.currentTimeMillis();
        job.waitForCompletion(true);
        long t2 = System.currentTimeMillis();
        totalTime += t2 - t1;

        fs.copyToLocalFile(new Path(hdfsPaths[1] + "part-r-00000"), new Path(paths[2]));
        System.err.println("-----PHASE 1 COMPLETED-----");

    }

    private void phaseThree(Configuration conf, String[] paths)
            throws Exception {

        hdfsFileInit = hdfsFileInit.concat(paths[5]);
        conf.set("fs.defaultFS", hdfsFileInit);
        String hdfsPort = hdfsFileInit;
        hdfsFileInit = hdfsFileInit.concat(paths[6]);

        Job job = Job.getInstance();
        job.setJarByClass(JobRunner.class);
        job.setJobName("MiningPhase");


        K = Integer.parseInt(paths[4]);
        minUtility = Long.parseLong(paths[3]);

        job.setMapperClass(MiningPhaseMapper.class);

        job.setReducerClass(MiningPhaseReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem fs = FileSystem.get(conf);

        String[] hdfsPaths = {hdfsFileInit + "phase3input/", hdfsFileInit + "phase3output/"};

        /**
         * Creates the input directory, if it doesn't exist.
         */
        if (!fs.exists(new Path(hdfsPaths[0]))) {
            fs.mkdirs(new Path(hdfsPaths[0]));
        }
        /**
         * Deletes the output folder, if it exists.
         */
        if (fs.exists(new Path(hdfsPaths[1]))) {
            fs.delete(new Path(hdfsPaths[1]), true);
        }
        /**
         * Copy the dataset the local folder to the HDFS file system.
         */
        fs.copyFromLocalFile(false, true, new Path(paths[1]), new Path(hdfsPaths[0]));

        TextInputFormat.setInputPaths(job, hdfsPaths[0] + new Path(paths[1]).getName());
        TextOutputFormat.setOutputPath(job, new Path(hdfsPaths[1]));

        long t1 = System.currentTimeMillis();
        job.waitForCompletion(true);
        long t2 = System.currentTimeMillis();
        totalTime += t2 - t1;

        fs.copyToLocalFile(new Path(hdfsPaths[1] + "part-r-00000"), new Path(paths[2]));
        
        for (int k = 2; k <= K; k++) {

            conf.set("fs.defaultFS", hdfsPort);

            job = Job.getInstance();
            job.setJarByClass(JobRunner.class);

            job.setJobName("MiningPhase");
            System.out.println(k + "th Iteration");

            job.setMapperClass(KthMiningPhaseMapper.class);
            job.setReducerClass(KthMiningPhaseReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            fs = FileSystem.get(conf);
            hdfsPaths = new String[]{hdfsFileInit + "phase3input/", hdfsFileInit + "phase3output/"};

            /**
             * Creates the input directory, if it doesn't exist.
             */
            if (!fs.exists(new Path(hdfsPaths[0]))) {
                fs.mkdirs(new Path(hdfsPaths[0]));
            }
            /**
             * Deletes the output folder, if it exists.
             */
            if (fs.exists(new Path(hdfsPaths[1]))) {
                fs.delete(new Path(hdfsPaths[1]), true);
            }
            /**
             * Copy the dataset the local folder to the HDFS file system.
             */
            fs.copyFromLocalFile(false, true, new Path(paths[2]), new Path(hdfsPaths[0]));
            System.out.println("path  = " + hdfsPaths[0] + new Path(paths[2]).getName());

            TextInputFormat.setInputPaths(job, hdfsPaths[0] + new Path(paths[2]).getName());
            TextOutputFormat.setOutputPath(job, new Path(hdfsPaths[1]));

            t1 = System.currentTimeMillis();
            job.waitForCompletion(true);
            t2 = System.currentTimeMillis();
            totalTime += t2 - t1;

            fs.copyToLocalFile(new Path(hdfsPaths[1] + "part-r-00000"), new Path(paths[2]));

        }
        
        

    }

}
