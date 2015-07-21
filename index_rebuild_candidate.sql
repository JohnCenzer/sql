--
-- Based on Metalink: Script to investigate a b-tree index structure (Doc ID 989186.1)
--
/*
alter session enable parallel dml;
alter session set sort_area_size= 1073741824 ;
alter session set ddl_lock_timeout=10;
*/
WITH global_params AS
(SELECT /*+MATERIALIZE*/
        DISTINCT 
        192    AS vOverhead -- leaf block "lost" space in index_stats
       ,2000   AS vMinBlks 
       ,0.6    AS vScaleFactor
       ,90     AS vTargetUse    
       ,8192   AS vBlockSize
       ,100/90 AS vAssumedPackingEfficiency
       ,'NOLOGGING'  AS no_logging_clause
       ,'LOGGING'    AS logging_clause 
       ,'32'         AS rebuild_degree
       ,'32'         AS stats_degree
  FROM dual)
,tab AS 
(SELECT /*+MATERIALIZE*/
        DISTINCT
        dt.table_name
       ,dt.last_analyzed
       ,dt.partitioned
       ,dt.num_rows
   FROM user_tables dt, 
        global_params gp
  WHERE 1=1
    AND dt.table_name NOT LIKE '%BKP'
    AND dt.table_name NOT LIKE '%BKUP'
    AND dt.table_name NOT LIKE 'TEMP%'
    AND dt.table_name NOT LIKE '%TEMP'
    AND dt.table_name NOT LIKE '%TEST%'
    AND dt.table_name NOT LIKE '%TST%'
    AND dt.table_name NOT LIKE '%DROP%ME%'
    AND SUBSTR( dt.table_name, -1,1 ) NOT IN ('1', '2', '3', '4', '5', '6', '7', '8', '9', '0')
    AND SUBSTR( dt.table_name, -2,2 ) NOT IN ( '_N','_X' )
    AND SUBSTR( dt.table_name, -3,3 ) NOT IN ( '_JC' )
    AND SUBSTR( dt.table_name, -4,4 ) <> '_OLD'
    AND dt.temporary = 'N'
    AND dt.dropped = 'NO'
    AND dt.segment_created = 'YES'
    AND EXISTS
              (SELECT 1
                 FROM user_indexes di
                WHERE di.table_name = dt.table_name
                  AND di.index_type IN ('NORMAL', 'NORMAL/REV', 'FUNCTION-BASED NORMAL')
                  AND di.partitioned = 'NO'
                  AND di.temporary = 'N'
                  AND di.dropped = 'NO'
                  AND di.status = 'VALID'
                  AND di.last_analyzed IS NOT NULL
                  AND di.leaf_blocks > 0)
)
SELECT table_name,
       last_analyzed,
       index_name,
       ind_last_analyzed,
       ind_last_ddl_time,
       ind_ini_trans,
       tablespace_name,
       uniqueness,
       column_names,
       ind_num_rows          "IND NUM ROWS",
       MB,
       blocks,
       leaf_blocks           "LEAF BLOCKS",
       index_leaf_estimate,
       pct_size_est          "PCT SIZE EST",
       rebuildCase1          "PCT SIZE EST < .60",
       leaf_blocks_per_block "LEAF BLOCKS PER BLOCK",
       rebuildCase2          "LEAF BLOCKS PER BLOCK < .60",
       rebuild,
       est_mb_change         "EST MB CHANGE",
       est_block_change      "EST BLOCK CHANGE"
       --
       --/*
      ,'Prompt ' || USER ||'.'||index_name ||CHR(10)||
       'ALTER INDEX ' || USER ||'.'||index_name ||' REBUILD ONLINE '||no_logging_clause||';'||CHR(10)||
       'ALTER INDEX ' || USER ||'.'||index_name ||' '||logging_clause||';'||CHR(10)||
       'EXEC DBMS_STATS.GATHER_INDEX_STATS(USER,'''||index_name||''',degree=>'||stats_degree||',force=>TRUE);'||CHR(10) "Rebuild Script",
       --
       'Prompt ' || USER ||'.'||index_name ||CHR(10)||
       'ALTER INDEX ' || USER ||'.'||index_name ||' REBUILD ONLINE PARALLEL( DEGREE '||rebuild_degree||' ) '||no_logging_clause||';'||CHR(10)||
       'ALTER INDEX ' || USER ||'.'||index_name ||' PARALLEL( DEGREE '||ind_degree||' ) '||logging_clause||';'||CHR(10)||
       'EXEC DBMS_STATS.GATHER_INDEX_STATS(USER,'''||index_name||''',degree=>'||stats_degree||',force=>TRUE);'||CHR(10) "Parallel Rebuild Script",
       --
       'Prompt ' || USER ||'.'||index_name ||CHR(10)||
       'ALTER INDEX ' || USER ||'.'||index_name ||' SHRINK SPACE COMPACT;'||CHR(10)||
       'ALTER INDEX ' || USER ||'.'||index_name ||' SHRINK SPACE;'||CHR(10)||
       'EXEC DBMS_STATS.GATHER_INDEX_STATS(USER,'''||index_name||''',degree=>'||stats_degree||',force=>TRUE);'||CHR(10) "Shrink Script"
       --*/
       --
  FROM (
  SELECT table_name
        ,last_analyzed
        ,(SELECT last_ddl_time FROM user_objects d WHERE d.object_name = index_name  ) AS ind_last_ddl_time
        ,index_name
        ,ind_last_analyzed
        ,ind_ini_trans
        ,tablespace_name
        ,column_names
        ,uniqueness
        ,uniq_ind
        ,TO_CHAR( ind_num_rows,              'B999,999,999,999') AS ind_num_rows
        ,TO_CHAR( ROUND(bytes/1024/1024,0),  'B999,999')         AS mb
        ,TO_CHAR( blocks,                    'B999,999,999')     AS blocks
        ,TO_CHAR( leaf_blocks,               'B999,999,999,999') AS leaf_blocks
        ,TO_CHAR( index_leaf_estimate,       'B999,999,999,999') AS index_leaf_estimate
        ,ROUND( index_leaf_estimate / leaf_blocks, 2 )           AS pct_size_est
        ,ROUND( leaf_blocks / blocks,2 )                         AS leaf_blocks_per_block
        ,CASE WHEN index_leaf_estimate < leaf_blocks * vScaleFactor THEN 'Y' ELSE 'N' END AS rebuildCase1
        ,CASE WHEN leaf_blocks < blocks * vScaleFactor              THEN 'Y' ELSE 'N' END AS rebuildCase2
        ,CASE WHEN index_leaf_estimate < leaf_blocks * vScaleFactor THEN 'Y'
              WHEN leaf_blocks < blocks * vScaleFactor              THEN 'Y'
              ELSE 'N'
          END AS rebuild
         --
        ,CASE WHEN index_leaf_estimate < leaf_blocks * vScaleFactor OR
                   leaf_blocks < blocks * vScaleFactor              THEN
                TO_CHAR(
                   ROUND(-GREATEST( ROUND( bytes * (1- ROUND( index_leaf_estimate / leaf_blocks, 2 ) ), 2) ,
                                    ROUND( bytes * (1- ROUND( leaf_blocks / blocks,2 ) )              , 2) )/1024/1024,0), 'B999,999')
              ELSE NULL
          END AS est_mb_change
          --
        ,CASE WHEN index_leaf_estimate < leaf_blocks * vScaleFactor OR
                   leaf_blocks < blocks * vScaleFactor
              THEN
                TO_CHAR(
                   -GREATEST( ROUND( blocks * (1- ROUND( index_leaf_estimate / leaf_blocks, 2 ) ), 2) ,
                              ROUND( blocks * (1- ROUND( leaf_blocks / blocks,2 ) )              , 2) ), 'B999,999,999,999')
              ELSE NULL
          END AS est_block_change
          --
         ,no_logging_clause
         ,logging_clause
         ,rebuild_degree
         ,ind_degree
         ,stats_degree
    FROM (  SELECT table_name
                  ,last_analyzed
                  ,index_name
                  ,ind_last_analyzed
                  ,tablespace_name
                  ,listagg( column_name,', ') WITHIN GROUP (order by column_position) AS column_names
                  ,tab_num_rows
                  ,ind_num_rows
                  ,ind_ini_trans                  
                  ,uniqueness
                  ,uniq_ind
                  ,rowid_length
                  ,blocks
                  ,bytes
                  ,leaf_blocks
                  ,ROUND ( vAssumedPackingEfficiency * 
                           ( ind_num_rows * (rowid_length + uniq_ind + 4) + SUM ( (avg_col_len) * (tab_num_rows)) -- column data bytes
                            ) / (vBlockSize - vOverhead - (ind_ini_trans*23))
                          ) index_leaf_estimate
                  ,vScaleFactor
                  ,db_name
                  ,no_logging_clause
                  ,logging_clause
                  ,rebuild_degree
                  ,ind_degree
                  ,stats_degree
              FROM (SELECT /*+ no_merge FIRST_ROWS(30)*/
                           tab.table_name      
                          ,tab.last_analyzed                     
                          ,DECODE (tab.partitioned, 'YES', 10, 6) AS rowid_length
                          ,tab.num_rows      AS tab_num_rows
                          ,ind.index_name
                          ,ind.last_analyzed AS ind_last_analyzed                  
                          ,ind.tablespace_name
                          ,ic.column_name
                          ,ic.column_position
                          ,ind.degree   AS ind_degree
                          ,ind.index_type
                          ,ind.uniqueness
                          ,DECODE (ind.uniqueness, 'UNIQUE', 0, 1) AS uniq_ind
                          ,ind.leaf_blocks
                          ,us.blocks
                          ,us.bytes
                          ,ind.num_rows      AS ind_num_rows
                          ,ind.ini_trans     AS ind_ini_trans
                          --,ROUND(ind.num_rows /tab.num_rows,2) pct_of_tab_in_idx
                          ,tc.avg_col_len
                          ,gp.vOverhead -- leaf block "lost" space in index_stats
                          ,gp.vMinBlks 
                          ,gp.vScaleFactor
                          ,gp.vTargetUse    
                          ,gp.vBlockSize
                          ,gp.vAssumedPackingEfficiency
                          ,gp.db_name
                          ,gp.no_logging_clause
                          ,gp.logging_clause
                          ,gp.rebuild_degree
                          ,gp.stats_degree
                      FROM global_params gp
                          ,tab
                      JOIN user_indexes ind
                        ON tab.table_name    = ind.table_name AND                            
                           ind.index_type IN ('NORMAL',
                                              'NORMAL/REV',
                                              'FUNCTION-BASED NORMAL') AND
                           ind.partitioned   = 'NO' AND
                           ind.temporary     = 'N' AND
                           ind.dropped       = 'NO' AND
                           ind.status        = 'VALID' AND
                           ind.last_analyzed IS NOT NULL AND
                           ind.leaf_blocks   > 0                     
                      JOIN user_tab_cols tc
                        ON tab.table_name    = tc.table_name 
                      JOIN user_ind_columns ic
                        ON tc.column_name  = ic.column_name AND
                           ind.index_name  = ic.index_name 
                      JOIN user_segments us
                        ON ind.index_name  = us.segment_name AND  
                           us.segment_type = 'INDEX'
                     WHERE ( ind.leaf_blocks > gp.vMinBlks OR
                             us.blocks       > gp.vMinBlks 
                            )                     
                       )
          GROUP BY table_name
                  ,last_analyzed
                  ,index_name
                  ,ind_last_analyzed
                  ,tablespace_name
                  ,tab_num_rows
                  ,ind_num_rows
                  ,ind_ini_trans
                  ,uniqueness
                  ,uniq_ind
                  ,rowid_length
                  ,blocks
                  ,bytes
                  ,leaf_blocks
                  ,vScaleFactor
                  ,vAssumedPackingEfficiency
                  ,vBlockSize
                  ,vOverhead
                  ,vScaleFactor
                  ,db_name
                  ,no_logging_clause
                  ,logging_clause
                  ,rebuild_degree
                  ,ind_degree
                  ,stats_degree)
 ) t
WHERE rebuild='Y' 
  AND TO_NUMBER(TRIM(REPLACE(blocks,',','')))>2000
ORDER BY 
       table_name,
       index_name
/

  