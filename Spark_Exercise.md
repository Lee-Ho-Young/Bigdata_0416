**통신QM Unit 이호영 선임(09340)**

<Exercise. Process Data Files with Apache Spark>
-------------------------
**Parsing XML file with Spark**

**1. Data Repository**

```
[training@localhost activations]$ cd /home/training/training_materials/data/activations
[training@localhost activations]$ ll
total 49632
-rw-r--r-- 1 training training   19044 Nov 14  2016 2008-10.xml
-rw-r--r-- 1 training training   73748 Nov 14  2016 2008-11.xml
-rw-r--r-- 1 training training   68764 Nov 14  2016 2008-12.xml
-rw-r--r-- 1 training training   90337 Nov 14  2016 2009-01.xml
-rw-r--r-- 1 training training   67655 Nov 14  2016 2009-02.xml
-rw-r--r-- 1 training training   86243 Nov 14  2016 2009-03.xml
-rw-r--r-- 1 training training   82718 Nov 14  2016 2009-04.xml
-rw-r--r-- 1 training training   90148 Nov 14  2016 2009-05.xml
-rw-r--r-- 1 training training   88713 Nov 14  2016 2009-06.xml
-rw-r--r-- 1 training training   86835 Nov 14  2016 2009-07.xml
-rw-r--r-- 1 training training   79114 Nov 14  2016 2009-08.xml
-rw-r--r-- 1 training training   83012 Nov 14  2016 2009-09.xml
-rw-r--r-- 1 training training   91007 Nov 14  2016 2009-10.xml
-rw-r--r-- 1 training training   93132 Nov 14  2016 2009-11.xml
-rw-r--r-- 1 training training   96328 Nov 14  2016 2009-12.xml
-rw-r--r-- 1 training training  192394 Nov 14  2016 2010-01.xml
-rw-r--r-- 1 training training  191631 Nov 14  2016 2010-02.xml
-rw-r--r-- 1 training training  200690 Nov 14  2016 2010-03.xml
-rw-r--r-- 1 training training  200510 Nov 14  2016 2010-04.xml
-rw-r--r-- 1 training training  227033 Nov 14  2016 2010-05.xml
-rw-r--r-- 1 training training  235350 Nov 14  2016 2010-06.xml
-rw-r--r-- 1 training training  230267 Nov 14  2016 2010-07.xml
-rw-r--r-- 1 training training  241761 Nov 14  2016 2010-08.xml
-rw-r--r-- 1 training training  234168 Nov 14  2016 2010-09.xml
-rw-r--r-- 1 training training  237083 Nov 14  2016 2010-10.xml
-rw-r--r-- 1 training training  209493 Nov 14  2016 2010-11.xml
-rw-r--r-- 1 training training  235910 Nov 14  2016 2010-12.xml
-rw-r--r-- 1 training training  441580 Nov 14  2016 2011-01.xml
-rw-r--r-- 1 training training  421089 Nov 14  2016 2011-02.xml
-rw-r--r-- 1 training training  472902 Nov 14  2016 2011-03.xml
-rw-r--r-- 1 training training  456871 Nov 14  2016 2011-04.xml
-rw-r--r-- 1 training training  466384 Nov 14  2016 2011-05.xml
-rw-r--r-- 1 training training  454844 Nov 14  2016 2011-06.xml
-rw-r--r-- 1 training training  466854 Nov 14  2016 2011-07.xml
-rw-r--r-- 1 training training  483014 Nov 14  2016 2011-08.xml
-rw-r--r-- 1 training training  464367 Nov 14  2016 2011-09.xml
-rw-r--r-- 1 training training  500909 Nov 14  2016 2011-10.xml
-rw-r--r-- 1 training training  477224 Nov 14  2016 2011-11.xml
-rw-r--r-- 1 training training  506646 Nov 14  2016 2011-12.xml
-rw-r--r-- 1 training training  979534 Nov 14  2016 2012-01.xml
-rw-r--r-- 1 training training  945789 Nov 14  2016 2012-02.xml
-rw-r--r-- 1 training training 1010401 Nov 14  2016 2012-03.xml
-rw-r--r-- 1 training training  994863 Nov 14  2016 2012-04.xml
-rw-r--r-- 1 training training 1005624 Nov 14  2016 2012-05.xml
-rw-r--r-- 1 training training  957156 Nov 14  2016 2012-06.xml
-rw-r--r-- 1 training training 1028510 Nov 14  2016 2012-07.xml
-rw-r--r-- 1 training training 1055421 Nov 14  2016 2012-08.xml
-rw-r--r-- 1 training training 1003936 Nov 14  2016 2012-09.xml
-rw-r--r-- 1 training training 1066257 Nov 14  2016 2012-10.xml
-rw-r--r-- 1 training training 1000719 Nov 14  2016 2012-11.xml
-rw-r--r-- 1 training training 1045239 Nov 14  2016 2012-12.xml
-rw-r--r-- 1 training training 1081374 Nov 14  2016 2013-01.xml
-rw-r--r-- 1 training training  984057 Nov 14  2016 2013-02.xml
-rw-r--r-- 1 training training 1115803 Nov 14  2016 2013-03.xml
-rw-r--r-- 1 training training 1079565 Nov 14  2016 2013-04.xml
-rw-r--r-- 1 training training 1092603 Nov 14  2016 2013-05.xml
-rw-r--r-- 1 training training 1066438 Nov 14  2016 2013-06.xml
-rw-r--r-- 1 training training 1133909 Nov 14  2016 2013-07.xml
-rw-r--r-- 1 training training 1137010 Nov 14  2016 2013-08.xml
-rw-r--r-- 1 training training 1059769 Nov 14  2016 2013-09.xml
-rw-r--r-- 1 training training 1132497 Nov 14  2016 2013-10.xml
-rw-r--r-- 1 training training 6816957 Nov 14  2016 2013-11.xml
-rw-r--r-- 1 training training 3734204 Nov 14  2016 2013-12.xml
-rw-r--r-- 1 training training 3516581 Nov 14  2016 2014-01.xml
-rw-r--r-- 1 training training 2878103 Nov 14  2016 2014-02.xml
-rw-r--r-- 1 training training 1316093 Nov 14  2016 2014-03.xml
```

**2. Upload files to HDFS**

```
[training@localhost activations]$ hdfs dfs -put $DEVDATA/activations /loudacre/
[training@localhost activations]$ hdfs dfs -ls /loudacre/activations
Found 66 items
-rw-rw-rw-   1 training supergroup      19044 2019-04-15 01:49 /loudacre/activations/2008-10.xml
-rw-rw-rw-   1 training supergroup      73748 2019-04-15 01:49 /loudacre/activations/2008-11.xml
-rw-rw-rw-   1 training supergroup      68764 2019-04-15 01:49 /loudacre/activations/2008-12.xml
-rw-rw-rw-   1 training supergroup      90337 2019-04-15 01:49 /loudacre/activations/2009-01.xml
-rw-rw-rw-   1 training supergroup      67655 2019-04-15 01:49 /loudacre/activations/2009-02.xml
-rw-rw-rw-   1 training supergroup      86243 2019-04-15 01:49 /loudacre/activations/2009-03.xml
-rw-rw-rw-   1 training supergroup      82718 2019-04-15 01:49 /loudacre/activations/2009-04.xml
-rw-rw-rw-   1 training supergroup      90148 2019-04-15 01:49 /loudacre/activations/2009-05.xml
-rw-rw-rw-   1 training supergroup      88713 2019-04-15 01:49 /loudacre/activations/2009-06.xml
-rw-rw-rw-   1 training supergroup      86835 2019-04-15 01:49 /loudacre/activations/2009-07.xml
-rw-rw-rw-   1 training supergroup      79114 2019-04-15 01:49 /loudacre/activations/2009-08.xml
-rw-rw-rw-   1 training supergroup      83012 2019-04-15 01:49 /loudacre/activations/2009-09.xml
-rw-rw-rw-   1 training supergroup      91007 2019-04-15 01:49 /loudacre/activations/2009-10.xml
-rw-rw-rw-   1 training supergroup      93132 2019-04-15 01:49 /loudacre/activations/2009-11.xml
-rw-rw-rw-   1 training supergroup      96328 2019-04-15 01:49 /loudacre/activations/2009-12.xml
-rw-rw-rw-   1 training supergroup     192394 2019-04-15 01:49 /loudacre/activations/2010-01.xml
-rw-rw-rw-   1 training supergroup     191631 2019-04-15 01:49 /loudacre/activations/2010-02.xml
-rw-rw-rw-   1 training supergroup     200690 2019-04-15 01:49 /loudacre/activations/2010-03.xml
-rw-rw-rw-   1 training supergroup     200510 2019-04-15 01:49 /loudacre/activations/2010-04.xml
-rw-rw-rw-   1 training supergroup     227033 2019-04-15 01:49 /loudacre/activations/2010-05.xml
-rw-rw-rw-   1 training supergroup     235350 2019-04-15 01:49 /loudacre/activations/2010-06.xml
-rw-rw-rw-   1 training supergroup     230267 2019-04-15 01:49 /loudacre/activations/2010-07.xml
-rw-rw-rw-   1 training supergroup     241761 2019-04-15 01:49 /loudacre/activations/2010-08.xml
-rw-rw-rw-   1 training supergroup     234168 2019-04-15 01:49 /loudacre/activations/2010-09.xml
-rw-rw-rw-   1 training supergroup     237083 2019-04-15 01:49 /loudacre/activations/2010-10.xml
-rw-rw-rw-   1 training supergroup     209493 2019-04-15 01:49 /loudacre/activations/2010-11.xml
-rw-rw-rw-   1 training supergroup     235910 2019-04-15 01:49 /loudacre/activations/2010-12.xml
-rw-rw-rw-   1 training supergroup     441580 2019-04-15 01:49 /loudacre/activations/2011-01.xml
-rw-rw-rw-   1 training supergroup     421089 2019-04-15 01:49 /loudacre/activations/2011-02.xml
-rw-rw-rw-   1 training supergroup     472902 2019-04-15 01:49 /loudacre/activations/2011-03.xml
-rw-rw-rw-   1 training supergroup     456871 2019-04-15 01:49 /loudacre/activations/2011-04.xml
-rw-rw-rw-   1 training supergroup     466384 2019-04-15 01:49 /loudacre/activations/2011-05.xml
-rw-rw-rw-   1 training supergroup     454844 2019-04-15 01:49 /loudacre/activations/2011-06.xml
-rw-rw-rw-   1 training supergroup     466854 2019-04-15 01:49 /loudacre/activations/2011-07.xml
-rw-rw-rw-   1 training supergroup     483014 2019-04-15 01:49 /loudacre/activations/2011-08.xml
-rw-rw-rw-   1 training supergroup     464367 2019-04-15 01:49 /loudacre/activations/2011-09.xml
-rw-rw-rw-   1 training supergroup     500909 2019-04-15 01:49 /loudacre/activations/2011-10.xml
-rw-rw-rw-   1 training supergroup     477224 2019-04-15 01:49 /loudacre/activations/2011-11.xml
-rw-rw-rw-   1 training supergroup     506646 2019-04-15 01:49 /loudacre/activations/2011-12.xml
-rw-rw-rw-   1 training supergroup     979534 2019-04-15 01:49 /loudacre/activations/2012-01.xml
-rw-rw-rw-   1 training supergroup     945789 2019-04-15 01:49 /loudacre/activations/2012-02.xml
-rw-rw-rw-   1 training supergroup    1010401 2019-04-15 01:49 /loudacre/activations/2012-03.xml
-rw-rw-rw-   1 training supergroup     994863 2019-04-15 01:49 /loudacre/activations/2012-04.xml
-rw-rw-rw-   1 training supergroup    1005624 2019-04-15 01:49 /loudacre/activations/2012-05.xml
-rw-rw-rw-   1 training supergroup     957156 2019-04-15 01:49 /loudacre/activations/2012-06.xml
-rw-rw-rw-   1 training supergroup    1028510 2019-04-15 01:49 /loudacre/activations/2012-07.xml
-rw-rw-rw-   1 training supergroup    1055421 2019-04-15 01:49 /loudacre/activations/2012-08.xml
-rw-rw-rw-   1 training supergroup    1003936 2019-04-15 01:49 /loudacre/activations/2012-09.xml
-rw-rw-rw-   1 training supergroup    1066257 2019-04-15 01:49 /loudacre/activations/2012-10.xml
-rw-rw-rw-   1 training supergroup    1000719 2019-04-15 01:49 /loudacre/activations/2012-11.xml
-rw-rw-rw-   1 training supergroup    1045239 2019-04-15 01:49 /loudacre/activations/2012-12.xml
-rw-rw-rw-   1 training supergroup    1081374 2019-04-15 01:49 /loudacre/activations/2013-01.xml
-rw-rw-rw-   1 training supergroup     984057 2019-04-15 01:49 /loudacre/activations/2013-02.xml
-rw-rw-rw-   1 training supergroup    1115803 2019-04-15 01:49 /loudacre/activations/2013-03.xml
-rw-rw-rw-   1 training supergroup    1079565 2019-04-15 01:49 /loudacre/activations/2013-04.xml
-rw-rw-rw-   1 training supergroup    1092603 2019-04-15 01:49 /loudacre/activations/2013-05.xml
-rw-rw-rw-   1 training supergroup    1066438 2019-04-15 01:49 /loudacre/activations/2013-06.xml
-rw-rw-rw-   1 training supergroup    1133909 2019-04-15 01:49 /loudacre/activations/2013-07.xml
-rw-rw-rw-   1 training supergroup    1137010 2019-04-15 01:49 /loudacre/activations/2013-08.xml
-rw-rw-rw-   1 training supergroup    1059769 2019-04-15 01:49 /loudacre/activations/2013-09.xml
-rw-rw-rw-   1 training supergroup    1132497 2019-04-15 01:49 /loudacre/activations/2013-10.xml
-rw-rw-rw-   1 training supergroup    6816957 2019-04-15 01:49 /loudacre/activations/2013-11.xml
-rw-rw-rw-   1 training supergroup    3734204 2019-04-15 01:49 /loudacre/activations/2013-12.xml
-rw-rw-rw-   1 training supergroup    3516581 2019-04-15 01:49 /loudacre/activations/2014-01.xml
-rw-rw-rw-   1 training supergroup    2878103 2019-04-15 01:49 /loudacre/activations/2014-02.xml
-rw-rw-rw-   1 training supergroup    1316093 2019-04-15 01:49 /loudacre/activations/2014-03.xml
```


**3. Processing RDD**

```
> rawRDD = sc.wholeTextFiles("/loudacre/activations/")
> rawRDD.take(2)

> rawRDD1 = rawRDD.flatMapValues(lambda line: getActivations(line))
> rawRDD1.take(2)
[(u'hdfs://localhost:8020/loudacre/activations/2008-10.xml',
  <Element 'activation' at 0x7f72f02e0390>),
 (u'hdfs://localhost:8020/loudacre/activations/2008-10.xml',
  <Element 'activation' at 0x7f72f02e06d0>)]
  
> rawRDD2 = rawRDD1.map(lambda (k, v): (getAccount(v), getModel(v)))
> rawRDD2.take(2)
[('9763', 'MeeToo 1.0'), ('426', 'Titanic 1000')]

> rawRDD3 = rawRDD2.map(lambda (k,v): k + ":" + v)
> rawRDD3.take(2)
['9763:MeeToo 1.0', '426:Titanic 1000']

> rawRDD3.saveAsTextFile("/loudacre/account-models")
```

