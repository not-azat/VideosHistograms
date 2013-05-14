#!/bin/sh
hadoop fs -rmr video_hist_output
hadoop jar VideosHist.jar -conf config/hadoop_localhost.xml -Dmapred.reduce.tasks=3 -Dmapred.job.reuse.jvm.num.tasks=-1 -Dmapred.tasktracker.map.tasks.maximum=4 -Dmapred.tasktracker.reduce.tasks.maximum=3 video_medium_input video_hist_output 6