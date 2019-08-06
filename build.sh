#!/usr/bin/env bash

PATHJAR=/home/d.skakun/build/libs
PATHDATA=/home/d.skakun/data

HADOOPIN=/data/infopoisk/hits_pagerank/docs-*
HADOOPURL=/data/infopoisk/hits_pagerank/urls.txt
HADOOPOUT=out/hw_Hits_PR/

PATH_GRAPH=graph
PATH_HITS=hits
PATH_HITS_H=h
PATH_HITS_A=a
PATH_PR=pr
PATH_PR_P=p

RESULT_GRAPH=graph.txt
RESULT_HITS=hits.txt
RESULT_H=hits_h.txt
RESULT_A=hits_a.txt
RESULT_PR=pagerank.txt
RESULT_P=pagerank_p.txt

# Строим граф
hadoop jar $PATHJAR/4_PageRank.jar Graph $HADOOPIN $HADOOPOUT/$PATH_GRAPH $HADOOPURL
hadoop fs -mv $HADOOPOUT/$PATH_GRAPH/part-r-* $HADOOPOUT/$PATH_GRAPH/$RESULT_GRAPH
hadoop fs -get $HADOOPOUT/$PATH_GRAPH/$RESULT_GRAPH $PATHDATA

# Считаем Hits
hadoop jar $PATHJAR/4_PageRank.jar Hits $HADOOPOUT/$PATH_GRAPH/$RESULT_GRAPH $HADOOPOUT/$PATH_HITS
hadoop fs -mv $HADOOPOUT/$PATH_HITS/iter_last/part-r-* $HADOOPOUT/$PATH_HITS/iter_last/$RESULT_HITS
hadoop fs -get $HADOOPOUT/$PATH_HITS/iter_last/$RESULT_HITS $PATHDATA

# Сортируем по H
hadoop jar $PATHJAR/4_PageRank.jar SortJob $HADOOPOUT/$PATH_HITS/iter_last/$RESULT_HITS $HADOOPOUT/$PATH_HITS/$PATH_HITS_H h
hadoop fs -mv $HADOOPOUT/$PATH_HITS/$PATH_HITS_H/part-r-* $HADOOPOUT/$PATH_HITS/$PATH_HITS_H/$RESULT_H
hadoop fs -get $HADOOPOUT/$PATH_HITS/$PATH_HITS_H/$RESULT_H $PATHDATA

# Сортируем по A
hadoop jar $PATHJAR/4_PageRank.jar SortJob $HADOOPOUT/$PATH_HITS/iter_last/$RESULT_HITS $HADOOPOUT/$PATH_HITS/$PATH_HITS_A a
hadoop fs -mv $HADOOPOUT/$PATH_HITS/$PATH_HITS_A/part-r-* $HADOOPOUT/$PATH_HITS/$PATH_HITS_A/$RESULT_A
hadoop fs -get $HADOOPOUT/$PATH_HITS/$PATH_HITS_A/$RESULT_A $PATHDATA

# Считаем PageRank
hadoop jar $PATHJAR/4_PageRank.jar PageRank $HADOOPOUT/$PATH_GRAPH/$RESULT_GRAPH $HADOOPOUT/$PATH_PR
hadoop fs -mv $HADOOPOUT/$PATH_PR/iter_last/part-r-* $HADOOPOUT/$PATH_PR/iter_last/$RESULT_PR
hadoop fs -get $HADOOPOUT/$PATH_PR/iter_last/$RESULT_PR $PATHDATA

# Сортируем PageRank
hadoop jar $PATHJAR/4_PageRank.jar SortJob $HADOOPOUT/$PATH_PR/iter_last/$RESULT_PR $HADOOPOUT/$PATH_PR/$PATH_PR_P p
hadoop fs -mv $HADOOPOUT/$PATH_PR/$PATH_PR_P/part-r-* $HADOOPOUT/$PATH_PR/$PATH_PR_P/$RESULT_P
hadoop fs -get $HADOOPOUT/$PATH_PR/$PATH_PR_P/$RESULT_P $PATHDATA