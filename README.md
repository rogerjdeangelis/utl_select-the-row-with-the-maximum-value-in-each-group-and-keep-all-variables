# utl_select-the-row-with-the-maximum-value-in-each-group-and-keep-all-variables
Select the row with the maximum value in each group and keep all variables  
    %let pgm=utl_select-the-row-with-the-maximum-value-in-each-group-and-keep-all-variables;

    Select the row with the maximum value in each group and keep all variables

    https://stackoverflow.com/questions/24558328/select-the-row-with-the-maximum-value-in-each-group

     Solutions
          1. WPS proc sql     output dataframe
          2. SAS proc sql     output dataframe
          3. WPS proc r sql   output dataframe
          4. WPS R data.table output dataframe  (about a dozenn more R solutions)
             https://stackoverflow.com/users/559784/arun (shortest code)
          5. Python SQL (same as WPS, SAS and R cretes a panda dataframe )

    github
    https://tinyurl.com/5j3by245
    https://github.com/rogerjdeangelis/utl_select-the-row-with-the-maximum-value-in-each-group-and-keep-all-variables
    /*                   _
    (_)_ __  _ __  _   _| |_
    | | `_ \| `_ \| | | | __|
    | | | | | |_) | |_| | |_
    |_|_| |_| .__/ \__,_|\__|
            |_|
    */


    options validvarname=upcase;
    data have;
     input Subject pt event;
    cards4;
    1 2 1
    1 3 1
    1 5 2
    2 2 1
    2 5 2
    2 8 1
    2 17 2
    3 3 2
    3 5 2
    ;;;;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                       |                                                                */
    /* Up to 40 obs from HAVE total obs=9 01JUN2023:12:52:45 |   RUES                                                         */
    /*                                                       |                                                                */
    /* Obs    SUBJECT    PT    EVENT                         |    Subject pt Event                                            */
    /*                                                       |          1  2     1                                            */
    /*  1        1        2      1                           |          1  3     1                                            */
    /*  2        1        3      1                           |          1  5     2 # max 'pt' for Subject 1                   */
    /*  3        1        5      2                           |          2  2     1                                            */
    /*  4        2        2      1                           |          2  5     2                                            */
    /*  5        2        5      2                           |          2  8     1                                            */
    /*  6        2        8      1                           |          2 17     2 # max 'pt' for Subject 2                   */
    /*  7        2       17      2                           |          3  3     2                                            */
    /*  8        3        3      2                           |          3  5     2 # max 'pt' for Subject 3                   */
    /*  9        3        5      2                           |                                                                */
    /*                                                       |                                                                */
    /**************************************************************************************************************************/

    /*           _               _
      ___  _   _| |_ _ __  _   _| |_
     / _ \| | | | __| `_ \| | | | __|
    | (_) | |_| | |_| |_) | |_| | |_
     \___/ \__,_|\__| .__/ \__,_|\__|
                    |_|
    */

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  Up to 40 obs from last table WORK.WANT total obs=3 01JUN2023:12:55:26                                                 */
    /*                                                                                                                        */
    /*  Obs    SUBJECT    MAXPT    EVENT                                                                                      */
    /*                                                                                                                        */
    /*   1        1          5       2                                                                                        */
    /*   2        2         17       2                                                                                        */
    /*   3        3          5       2                                                                                        */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*
    / |   __      ___ __  ___   _ __  _ __ ___   ___   ___  __ _
    | |   \ \ /\ / / `_ \/ __| | `_ \| `__/ _ \ / __| / __|/ _` |
    | |_   \ V  V /| |_) \__ \ | |_) | | | (_) | (__  \__ \ (_| |
    |_( )   \_/\_/ | .__/|___/ | .__/|_|  \___/ \___| |___/\__, |
      |/           |_|         |_|                            |_|
    */

    proc dataset lib=work nodetails nolist;
     delete want_wps_proc_sql;
    run;quit;

    %let _pth=%sysfunc(pathname(work));

    %utl_submit_wps64("
    options validvarname=any;
    libname wrk '&_pth';
    proc sql;
      create
         table wrk.want_wps_proc_sql as
      select
         subject
        ,max(pt) as maxPt
        ,event
      from
         wrk.have
      group
         by subject
      having
        max(pt) = pt
    ;quit;
    ");

    proc print data=want_wps_proc_sql;
    run;quit;

    /*___                                                       _
    |___ \     ___  __ _ ___   _ __  _ __ ___   ___   ___  __ _| |
      __) |   / __|/ _` / __| | `_ \| `__/ _ \ / __| / __|/ _` | |
     / __/ _  \__ \ (_| \__ \ | |_) | | | (_) | (__  \__ \ (_| | |
    |_____(_) |___/\__,_|___/ | .__/|_|  \___/ \___| |___/\__, |_|
                              |_|                            |_|
    */

    proc dataset lib=work nodetails nolist;
     delete want_sas_proc_sql;
    run;quit;

    proc sql;
      create
         table want_sas_proc_sql as
      select
         subject
        ,max(pt) as maxPt
        ,event
      from
         have
      group
         by subject
      having
        max(pt) = pt
    ;quit;

    /*____   __        ______  ____                                            _
    |___ /   \ \      / /  _ \/ ___|   _ __  _ __ ___   ___   _ __   ___  __ _| |
      |_ \    \ \ /\ / /| |_) \___ \  | `_ \| `__/ _ \ / __| | `__| / __|/ _` | |
     ___) |    \ V  V / |  __/ ___) | | |_) | | | (_) | (__  | |    \__ \ (_| | |
    |____(_)    \_/\_/  |_|   |____/  | .__/|_|  \___/ \___| |_|    |___/\__, |_|
                                      |_|                                   |_|
    */

    %let _pth=%sysfunc(pathname(work));

    %utl_submit_wps64("
    options validvarname=any;
    libname wrk '&_pth';
     proc r;
     export data=wrk.have r=have;
     submit;
     library(sqldf);
     want_wps_proc_r_sql<-sqldf('
      select
         subject
        ,max(pt) as maxPt
        ,event
      from
         have
      group
         by subject
      having
        max(pt) = pt
      ');
     want_wps_proc_r_sql;
    endsubmit;
    ");

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  The WPS System                                                                                                        */
    /*                                                                                                                        */
    /*    SUBJECT maxPt EVENT                                                                                                 */
    /*  1       1     5     1                                                                                                 */
    /*  2       2    17     2                                                                                                 */
    /*  3       3     5     2                                                                                                 */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*                               _       _          _        _     _
    __      ___ __  ___   _ __    __| | __ _| |_ __ _  | |_ __ _| |__ | | ___
    \ \ /\ / / `_ \/ __| | `__|  / _` |/ _` | __/ _` | | __/ _` | `_ \| |/ _ \
     \ V  V /| |_) \__ \ | |    | (_| | (_| | || (_| | | || (_| | |_) | |  __/
      \_/\_/ | .__/|___/ |_|     \__,_|\__,_|\__\__,_|  \__\__,_|_.__/|_|\___|
             |_|
    */

    proc dataset lib=work nodetails nolist;
     delete want_data_table_r;
    run;quit;

    %let _pth=%sysfunc(pathname(work));

    %utl_submit_wps64("
    options validvarname=any;
    libname wrk '&_pth';
     proc r;
     export data=wrk.have r=have;
     submit;
     library(data.table);
     have<-as.data.table(have);
     want_data_table_r <- setDT(have)[, .SD[which.max(PT)], by=SUBJECT];
     str(want_data_table_r);
     want_data_table_r;
    endsubmit;
    import data=wrk.want_data_table_r r=want_data_table_r ;
    ");

    proc print data=want_data_table_r;
    run;quit;

    /*___                 _   _                             _
    | ___|    _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
    |___ \   | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
     ___) |  | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
    |____(_) | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
             |_|    |___/                                |_|
    */

    libname sd1 "d:/sd1";

    options validvarname=upcase;

    data sd1.have;
     input Subject pt event;
    cards4;
    1 2 1
    1 3 1
    1 5 2
    2 2 1
    2 5 2
    2 8 1
    2 17 2
    3 3 2
    3 5 2
    ;;;;
    run;quit;

    proc datasets lib=work kill nodetails nolist;
    run;quit;

    %utlfkil(d:/xpt/res.xpt);

    /*---- all the extra sql statements are need to add missing sql functions  ----*/
    /*---- you can download c:/temp/libsqlitefunctions.dll from my macro repor ----*/

    %utl_pybegin;
    parmcards4;
    from os import path
    import pandas as pd
    import xport
    import xport.v56
    import pyreadstat
    import numpy as np
    import pandas as pd
    from pandasql import sqldf
    mysql = lambda q: sqldf(q, globals())
    from pandasql import PandaSQL
    pdsql = PandaSQL(persist=True)
    sqlite3conn = next(pdsql.conn.gen).connection.connection
    sqlite3conn.enable_load_extension(True)
    sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll')
    mysql = lambda q: sqldf(q, globals())
    have, meta = pyreadstat.read_sas7bdat("d:/sd1/have.sas7bdat")
    print(have);
    res = pdsql("""
      select
         subject
        ,max(pt) as maxPt
        ,event
      from
         have
      group
         by subject
      having
        max(pt) = pt
    """)
    print(res);
    ds = xport.Dataset(res, name='res')
    with open('d:/xpt/res.xpt', 'wb') as f:
        xport.v56.dump(ds, f)
    ;;;;
    %utl_pyend;

    libname pyxpt xport "d:/xpt/res.xpt";

    proc contents data=pyxpt._all_;
    run;quit;

    proc print data=pyxpt.res;
    run;quit;

    data res;
       set pyxpt.res;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*    SUBJECT  maxPt  EVENT                                                                                               */
    /* 0      1.0    5.0    2.0                                                                                               */
    /* 1      2.0   17.0    2.0                                                                                               */
    /* 2      3.0    5.0    2.0                                                                                               */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*              _
      ___ _ __   __| |
     / _ \ `_ \ / _` |
    |  __/ | | | (_| |
     \___|_| |_|\__,_|

    */
