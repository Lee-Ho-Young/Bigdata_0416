**통신QM Unit 이호영 선임(09340)**

<Lab2. Process Data Files with Apache Spark>
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
?? how can I use sortBy / sortByKey
```

**3. Bonus**
```diff
- Important Exercise
```
A. Upload the devicestatus.txt file to HDFS.

```
[training@localhost data]$ hdfs dfs -put devicestatus.txt /loudacre/device/
[training@localhost data]$ hdfs dfs -ls /loudacre/device
Found 1 items
-rw-rw-rw-   1 training supergroup   13954723 2019-04-15 20:58 /loudacre/device/devicestatus.txt
```

B. Determine which delimiter to use (hint: the character at
position 19 is the first use of the delimiter).

```
#Try to split the file with "," first
> files = "/loudacre/device/devicestatus.txt"
> rowRDD1 = sc.textFile(files)
> rowRDD1.take(2)
[u'2014-03-15:10:10:20,Sorrento F41L,8cc3b47e-bd01-4482-b500-28f2342679af,7,24,39,enabled,disabled,connected,55,67,12,33.6894754264,-117.543308253',
 u'2014-03-15:10:10:20|MeeToo 1.0|ef8c7564-0a1a-4650-a655-c8bbd5f8f943|0|31|63|70|39|27|enabled|enabled|enabled|37.4321088904|-121.485029632']

> rowRDD2 = rowRDD1.map(lambda line: line.split(","))
> rowRDD2.take(2)
[[u'2014-03-15:10:10:20',
  u'Sorrento F41L',
  u'8cc3b47e-bd01-4482-b500-28f2342679af',
  u'7',
  u'24',
  u'39',
  u'enabled',
  u'disabled',
  u'connected',
  u'55',
  u'67',
  u'12',
  u'33.6894754264',
  u'-117.543308253'],
 [u'2014-03-15:10:10:20|MeeToo 1.0|ef8c7564-0a1a-4650-a655-c8bbd5f8f943|0|31|63|70|39|27|enabled|enabled|enabled|37.4321088904|-121.485029632']]
```

C. Filter out any records which do not parse correctly (hint: each
record should have exactly 14 values).

```
> notCommaRDD = rowRDD2.filter(lambda line: len(line) != 14)
> notCommaRDD.take(2)
[[u'2014-03-15:10:10:20|MeeToo 1.0|ef8c7564-0a1a-4650-a655-c8bbd5f8f943|0|31|63|70|39|27|enabled|enabled|enabled|37.4321088904|-121.485029632'],
 [u'2014-03-15:10:10:20|MeeToo 1.0|23eba027-b95a-4729-9a4b-a3cca51c5548|0|20|21|86|54|34|enabled|enabled|enabled|39.4378908349|-120.938978486']]
  
> def ListMap(line):
    for arr in line:
        return arr.split("|")
        
> rowRDD3 = notCommaRDD.map(lambda line: ListMap(line))
> rowRDD3.take(2)
[[u'2014-03-15:10:10:20',
  u'MeeToo 1.0',
  u'ef8c7564-0a1a-4650-a655-c8bbd5f8f943',
  u'0',
  u'31',
  u'63',
  u'70',
  u'39',
  u'27',
  u'enabled',
  u'enabled',
  u'enabled',
  u'37.4321088904',
  u'-121.485029632'],
 [u'2014-03-15:10:10:20',
  u'MeeToo 1.0',
  u'23eba027-b95a-4729-9a4b-a3cca51c5548',
  u'0',
  u'20',
  u'21',
  u'86',
  u'54',
  u'34',
  u'enabled',
  u'enabled',
  u'enabled',
  u'39.4378908349',
  u'-120.938978486']]
```

D. Extract the date (first field), model (second field), device ID (third
field), and latitude and longitude (13th and 14th fields
respectively).

```
# rowRDD2 : Parsed with Comma + Unparsed data
# rowRDD3 : Parsing the unparsed data within rowRDD2 with | delimiter
# rowRDD4 : "," and "|" parsed data + Unparsed data
# rowRDD6 : All parsed data with "," "|" "/"
# rowRDD7 : Extract required data fields into one String
# rowRDD8 : Transform String data into array form

> rowRDD4 = rowRDD2.filter(lambda line: len(line) == 14).union(rowRDD3)
> rowRDD4.filter(lambda line: len(line) != 14).take(2)
[[u'2014-03-15:10:10:20/Titanic 2400/b4a15931-9a69-469f-9823-a45974472c51/21/96/63/38/11/0/enabled/disabled/enabled/38.1653163975/-122.151608378'],
 [u'2014-03-15:10:10:20/Titanic 2000/08bf61ec-f224-4e8c-a754-1ed381329ed4/43/80/28/61/20/0/enabled/enabled/connected/45.326414382/-117.807811103']]

> def ListMap2(line):
    for arr in line:
        return arr.split("/")

> rowRDD5 = rowRDD4.filter(lambda line: len(line) != 14).map(lambda line: ListMap2(line))
> rowRDD5.take(2)
[[u'2014-03-15:10:10:20',
  u'Titanic 2400',
  u'b4a15931-9a69-469f-9823-a45974472c51',
  u'21',
  u'96',
  u'63',
  u'38',
  u'11',
  u'0',
  u'enabled',
  u'disabled',
  u'enabled',
  u'38.1653163975',
  u'-122.151608378'],
 [u'2014-03-15:10:10:20',
  u'Titanic 2000',
  u'08bf61ec-f224-4e8c-a754-1ed381329ed4',
  u'43',
  u'80',
  u'28',
  u'61',
  u'20',
  u'0',
  u'enabled',
  u'enabled',
  u'connected',
  u'45.326414382',
  u'-117.807811103']]
 
 > rowRDD6 = rowRDD4.filter(lambda line: len(line) == 14).union(rowRDD5)
 > rowRDD6.filter(lambda line: len(line) != 14).count()
 0
 
 > rowRDD7 = rowRDD6.map(lambda line: line[0] + "," + line[1] + "," + line[2] + "," + line[12] + "," + line[13])
 > rowRDD8 = rowRDD7.map(lambda line: line.split(","))
 > rowRDD8.take(2)
 [[u'2014-03-15:10:10:20',
  u'Sorrento F41L',
  u'8cc3b47e-bd01-4482-b500-28f2342679af',
  u'33.6894754264',
  u'-117.543308253'],
 [u'2014-03-15:10:10:20',
  u'Sorrento F41L',
  u'707daba1-5640-4d60-a6d9-1d6fa0645be0',
  u'39.3635186767',
  u'-119.400334708']]
```

E. The second field contains the device manufacturer and model
name (such as Ronin S2). Split this field by spaces to separate
the manufacturer from the model (for example, manufacturer
Ronin, model S2). Keep just the manufacturer name.

```
> def processData(line):
    temp = line[1].split(' ')
    line[1] = temp[0]
    return line
        
> rowRDD9 = rowRDD8.map(lambda line: processData(line))
> rowRDD9.take(3)
[[u'2014-03-15:10:10:20',
  u'Sorrento',
  u'8cc3b47e-bd01-4482-b500-28f2342679af',
  u'33.6894754264',
  u'-117.543308253'],
 [u'2014-03-15:10:10:20',
  u'Sorrento',
  u'707daba1-5640-4d60-a6d9-1d6fa0645be0',
  u'39.3635186767',
  u'-119.400334708'],
 [u'2014-03-15:10:10:20',
  u'Ronin',
  u'db66fe81-aa55-43b4-9418-fc6e7a00f891',
  u'33.1913581092',
  u'-116.448242643']]
```

F. Save the extracted data to comma-delimited text files in the
/loudacre/devicestatus_etl directory on HDFS.

```
> commaTextRDD = rowRDD9.map(lambda line: line[0] + "," + line[1] + "," + line[2] + "," + line[3] + "," + line[4])
> commaTextRDD.take(2)
[u'2014-03-15:10:10:20,Sorrento,8cc3b47e-bd01-4482-b500-28f2342679af,33.6894754264,-117.543308253',
 u'2014-03-15:10:10:20,Sorrento,707daba1-5640-4d60-a6d9-1d6fa0645be0,39.3635186767,-119.400334708']
 
> commaTextRDD.saveAsTextFile("/loudacre/devicestatus_etl/")
```

G. Confirm that the data in the file(s) was saved correctly.

```
[training@localhost spark-etl]$ hdfs dfs -ls /loudacre/devicestatus_etl/
Found 5 items
-rw-rw-rw-   1 training supergroup          0 2019-04-16 01:23 /loudacre/devicestatus_etl/_SUCCESS
-rw-rw-rw-   1 training supergroup    6076596 2019-04-16 01:23 /loudacre/devicestatus_etl/part-00000
-rw-rw-rw-   1 training supergroup    1245363 2019-04-16 01:23 /loudacre/devicestatus_etl/part-00001
-rw-rw-rw-   1 training supergroup          0 2019-04-16 01:23 /loudacre/devicestatus_etl/part-00002
-rw-rw-rw-   1 training supergroup    1912147 2019-04-16 01:23 /loudacre/devicestatus_etl/part-00003

[training@localhost spark-etl]$ hdfs dfs -cat /loudacre/devicestatus_etl/part-00000
...
2014-03-15:10:49:30,Sorrento,86d93f67-0287-4e85-8472-076aa8b9fa42,37.4969347594,-122.174978527
2014-03-15:10:49:30,Sorrento,7c06751c-f692-473e-9143-9280594a9740,45.1387902196,-117.739092779
2014-03-15:10:49:30,Sorrento,40e61459-5448-4dc9-bb89-42e73a4e19cf,39.4463417571,-114.736213453
2014-03-15:10:49:30,Ronin,b13ece99-62ab-4c9f-a366-6a06bd5e877f,38.4282665514,-121.25933863
2014-03-15:10:49:30,Sorrento,32af1a0b-ca7f-4906-9772-9eb9435e7e4c,33.7778202246,-108.575470704
2014-03-15:10:49:30,Ronin,a48a5559-d916-481b-84a9-5dce6272cce1,38.2596913494,-122.295712621
2014-03-15:10:49:30,iFruit,d86fbaa6-b71b-435f-a0bf-5304a202a70b,34.2415255221,-118.23526739
...
```

<Lab3. Use Pair RDDs to Join Two Datasets>
-------------------------

**1. Parsing weblogs to find out visit frequency by user_id**

```
> logfiles="/loudacre/weblogs/*2.log"
> logsRDD1 = sc.textFile(logfiles)
> logsRDD1.take(2)
[u'131.166.169.114 - 67858 [23/Sep/2013:00:00:00 +0100] "GET /ifruit_3a_sales.html HTTP/1.0" 200 9509 "http://www.loudacre.com"  "Loudacre Mobile Browser Sorrento F01L"',
 u'131.166.169.114 - 67858 [23/Sep/2013:00:00:00 +0100] "GET /theme.css HTTP/1.0" 200 13428 "http://www.loudacre.com"  "Loudacre Mobile Browser Sorrento F01L"']
 
> logsRDD2 = logsRDD1.map(lambda line: line.split(' '))
> logsRDD2.take(2)
[[u'131.166.169.114',
  u'-',
  u'67858',
  u'[23/Sep/2013:00:00:00',
  u'+0100]',
  u'"GET',
  u'/ifruit_3a_sales.html',
  u'HTTP/1.0"',
  u'200',
  u'9509',
  u'"http://www.loudacre.com"',
  u'',
  u'"Loudacre',
  u'Mobile',
  u'Browser',
  u'Sorrento',
  u'F01L"'],
 [u'131.166.169.114',
  u'-',
  u'67858',
  u'[23/Sep/2013:00:00:00',
  u'+0100]',
  u'"GET',
  u'/theme.css',
  u'HTTP/1.0"',
  u'200',
  u'13428',
  u'"http://www.loudacre.com"',
  u'',
  u'"Loudacre',
  u'Mobile',
  u'Browser',
  u'Sorrento',
  u'F01L"']]
  
> logsRDD3 = logsRDD2.map(lambda line: (line[2],1))
> logsRDD3.take(2)
[(u'67858', 1), (u'67858', 1)]

> logsRDD4 = logsRDD3.reduceByKey(lambda v1,v2: v1+v2)
> logsRDD4.take(2)
[(u'3922', 6), (u'104959', 2)]

> logsRDD5 = logsRDD4.map(lambda (k,v): (v,k))
> logsRDD5.take(2)
[(6, u'3922'), (2, u'104959')]

> logsRDD6 = logsRDD5.countByKey()
> print(logsRDD6)
defaultdict(<type 'int'>, {128: 9, 2: 7239, 3: 36, 4: 4155, 5: 26, 6: 2162, 7: 14, 8: 1409, 9: 14, 10: 878, 11: 12, 12: 549, 13: 7, 14: 308, 15: 8, 16: 155, 17: 4, 146: 8, 19: 2, 20: 41, 21: 1, 22: 17, 150: 11, 152: 11, 132: 12, 154: 5, 27: 1, 156: 5, 158: 6, 160: 8, 162: 5, 164: 3, 134: 9, 166: 3, 168: 4, 170: 3, 172: 6, 174: 2, 24: 6, 176: 2, 136: 11, 178: 1, 188: 1, 138: 6, 190: 1, 130: 10, 140: 14, 142: 8, 86: 1, 144: 7, 100: 1, 104: 1, 106: 1, 18: 76, 110: 4, 112: 1, 116: 1, 118: 4, 120: 5, 148: 2, 122: 4, 124: 7, 126: 5})
```

**2. Parsing weblogs to find out (userID, ipList) pair**

```
> logfiles="/loudacre/weblogs/*2.log"
> logsRDD1 = sc.textFile(logfiles)
> logsRDD2 = logsRDD1.map(lambda line: line.split(' '))
> userIpRDD1 = logsRDD2.map(lambda line: (line[2], line[0]))
> userIpRDD1.take(2)
[(u'67858', u'131.166.169.114'), (u'67858', u'131.166.169.114')]

> userIpRDD2 = userIpRDD1.groupByKey()
> userIpRDD2.take(2)
[(u'3922', <pyspark.resultiterable.ResultIterable at 0x7f72f010d490>),
 (u'104959', <pyspark.resultiterable.ResultIterable at 0x7f72f00ebe90>)]
 
 
> userIpRDD3 = userIpRDD2.map(lambda (k,v): (k, list(v)))
> userIpRDD3.take(2)
[(u'3922',
  [u'195.220.211.104',
   u'195.220.211.104',
   u'138.217.174.182',
   u'138.217.174.182',
   u'138.217.174.182',
   u'138.217.174.182']),
 (u'104959', [u'183.123.205.115', u'183.123.205.115'])]
```

**3. Joining Web Log Data with Account Data**

```
> accountfiles="/loudacre/accounts/part*"
> accountRDD1 = sc.textFile(accountfiles)
> accountRDD1.take(2)
[u'1,2008-10-23 16:05:05.0,\\N,Donald,Becton,2275 Washburn Street,Oakland,CA,94660,5100032418,2014-03-18 13:29:47.0,2014-03-18 13:29:47.0',
 u'2,2008-11-12 03:00:01.0,\\N,Donna,Jones,3885 Elliott Street,San Francisco,CA,94171,4150835799,2014-03-18 13:29:47.0,2014-03-18 13:29:47.0']
 
> accountRDD2 = accountRDD1.keyBy(lambda line: line.split(',')[0])
> accountRDD2.take(2)
[(u'1',
  u'1,2008-10-23 16:05:05.0,\\N,Donald,Becton,2275 Washburn Street,Oakland,CA,94660,5100032418,2014-03-18 13:29:47.0,2014-03-18 13:29:47.0'),
 (u'2',
  u'2,2008-11-12 03:00:01.0,\\N,Donna,Jones,3885 Elliott Street,San Francisco,CA,94171,4150835799,2014-03-18 13:29:47.0,2014-03-18 13:29:47.0')]
  
> joinRDD1 = accountRDD2.join(logsRDD4)
> joinRDD1.take(2)
[(u'89371',
  (u'89371,2013-09-08 02:21:15.0,2014-01-19 12:17:06.0,Ricky,Pope,4535 Highland Drive,Portland,OR,97212,5033136196,2014-03-18 13:32:36.0,2014-03-18 13:32:36.0',
   4)),
 (u'99996',
  (u'99996,2013-03-14 19:19:45.0,2014-02-07 16:32:29.0,Garrett,Allen,495 Wilson Street,Prescott,AZ,86360,9280545713,2014-03-18 13:32:56.0,2014-03-18 13:32:56.0',
   2))]
   
> for (userid,(string,count)) in joinRDD1.take(5):
    print userid, count, string.split(",")[3], string.split(",")[4]
89371 4 Ricky Pope
99996 2 Garrett Allen
69171 6 Richard Tarver
90311 2 David Rosenberg
36848 6 Aaron Hutson

> joinRDD2 = joinRDD1
  .map(lambda (k,(v1,v2)): k + " " + str(v2) + " " + v1.split(",")[3] + " " + v1.split(",")[4])
> joinRDD2.take(2)
[u'89371 4 Ricky Pope', u'99996 2 Garrett Allen']
```

**4. Bonus**

1. Use keyBy to create an RDD of account data with the postal code (9th field in the CSV file) as the key.
   Tip: Assign this new RDD to a variable for use in the next bonus exercise.

```
> accountRDD1.take(2)
[u'1,2008-10-23 16:05:05.0,\\N,Donald,Becton,2275 Washburn Street,Oakland,CA,94660,5100032418,2014-03-18 13:29:47.0,2014-03-18 13:29:47.0',
 u'2,2008-11-12 03:00:01.0,\\N,Donna,Jones,3885 Elliott Street,San Francisco,CA,94171,4150835799,2014-03-18 13:29:47.0,2014-03-18 13:29:47.0']

> acntRDD1 = accountRDD1.keyBy(lambda line: line.split(',')[8])
> acntRDD1.take(2)
[(u'94660',
  u'1,2008-10-23 16:05:05.0,\\N,Donald,Becton,2275 Washburn Street,Oakland,CA,94660,5100032418,2014-03-18 13:29:47.0,2014-03-18 13:29:47.0'),
 (u'94171',
  u'2,2008-11-12 03:00:01.0,\\N,Donna,Jones,3885 Elliott Street,San Francisco,CA,94171,4150835799,2014-03-18 13:29:47.0,2014-03-18 13:29:47.0')]
```

2. Create a pair RDD with postal code as the key and a list of names (Last Name,First Name) in 
   that postal code as the value.

```
> acntRDD2 = acntRDD1.map(lambda (k,v): (k, (v.split(',')[4],v.split(',')[3])))
> acntRDD2.take(2)
[(u'94660', (u'Becton', u'Donald')), (u'94171', (u'Jones', u'Donna'))]

# Descending Order : acntRDD3 = acntRDD2.sortByKey(False)
> acntRDD3 = acntRDD2.sortByKey()
> acntRDD3.take(10)
[(u'85000', (u'Allen', u'Harvey')),
 (u'85000', (u'Prinz', u'Daniel')),
 (u'85000', (u'Pascale', u'Robert')),
 (u'85000', (u'Brookes', u'Donna')),
 (u'85000', (u'Mackenzie', u'James')),
 (u'85000', (u'Chamberlain', u'Robert')),
 (u'85000', (u'Cunningham', u'Richard')),
 (u'85000', (u'Sewell', u'Bailey')),
 (u'85000', (u'Marin', u'Daniel')),
 (u'85001', (u'Mendelsohn', u'Frances'))]
```

3. Sort the data by postal code, then for the first five postal codes, display
   the code and list the names in that postal zone.

--- 85003
Jenkins,Thad
Rick,Edward
Lindsay,Ivy
…
--- 85004
Morris,Eric
Reiser,Hazel
Gregg,Alicia
Preston,Elizabeth
…
   
```

```

<Lab4. Write and Run an Apache Spark Application>
-------------------------

**1. /home/training/training_materials/devsh/exercises/spark-application/CountJPGs.py**

```
import sys
from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print >> sys.stderr, "Usage: CountJPGs.py <logfile>"
        exit(-1)

    # Replace this line with your code:   
    sc = SparkContext()
    sc.setLogLevel("ERROR")
    logfiles = sys.argv[1]

    print("###################################################")
    print(logfiles)
    print("###################################################")

    logsRDD = sc.textFile(logfiles)
    cnt = logsRDD.filter(lambda line: ".jpg" in line).count()
    print("###################################################")
    print(cnt)
    print("###################################################")
    sc.stop()
```

**2. Change jupyter setting to normal pyspark**

**3. Running Program**

```
> spark-submit /home/training/training_materials/devsh/exercises/spark-application/CountJPGs.py /loudacre/weblogs/*

...
19/04/15 22:30:35 WARN util.Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
19/04/15 22:30:35 INFO server.Server: jetty-8.y.z-SNAPSHOT
19/04/15 22:30:35 INFO server.AbstractConnector: Started SelectChannelConnector@0.0.0.0:4041
19/04/15 22:30:35 INFO util.Utils: Successfully started service 'SparkUI' on port 4041.
19/04/15 22:30:35 INFO ui.SparkUI: Started SparkUI at http://192.168.2.146:4041
19/04/15 22:30:35 INFO util.Utils: Copying /home/training/training_materials/devsh/exercises/spark-application/CountJPGs.py to /tmp/spark-240ec17a-4a9c-4816-9d2d-29443b93fe7e/userFiles-5a18d278-666f-4478-8402-999e011bc089/CountJPGs.py
19/04/15 22:30:35 INFO spark.SparkContext: Added file file:/home/training/training_materials/devsh/exercises/spark-application/CountJPGs.py at file:/home/training/training_materials/devsh/exercises/spark-application/CountJPGs.py with timestamp 1555392635860
19/04/15 22:30:35 INFO executor.Executor: Starting executor ID driver on host localhost
19/04/15 22:30:36 INFO util.Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 34119.
19/04/15 22:30:36 INFO netty.NettyBlockTransferService: Server created on 34119
19/04/15 22:30:36 INFO storage.BlockManagerMaster: Trying to register BlockManager
19/04/15 22:30:36 INFO storage.BlockManagerMasterEndpoint: Registering block manager localhost:34119 with 208.8 MB RAM, BlockManagerId(driver, localhost, 34119)
19/04/15 22:30:36 INFO storage.BlockManagerMaster: Registered BlockManager
19/04/15 22:30:36 WARN shortcircuit.DomainSocketFactory: The short-circuit local reads feature cannot be used because libhadoop cannot be loaded.
19/04/15 22:30:37 INFO scheduler.EventLoggingListener: Logging events to hdfs:///user/spark/applicationHistory/local-1555392635937
###################################################
/loudacre/weblogs/*
###################################################
###################################################
64978
###################################################
```


<Lab5. Configure an Apache Spark Application>
-------------------------


<Lab6. View Jobs and Stages in the Spark Application UI>
-------------------------


<Lab7. Persist an RDD>
-------------------------

**1. Predefine script**

```
# Stub code to paste into the shell

sc.setLogLevel("WARN")

# Count web server log requests by user id
userReqs = sc.textFile("/loudacre/weblogs/*2.log")\
  .map(lambda line: line.split()) \
  .map(lambda words: (words[2],1)) \
  .reduceByKey(lambda v1,v2: v1+v2)

# Map account data to (userid,"lastname,firstname") pairs
accounts = sc.textFile("/loudacre/accounts/*")\
  .map(lambda s: s.split(',')) \
  .map(lambda values: (values[0],values[4] + ',' + values[3]))

# Join account names with request counts
accountHits=accounts.join(userReqs)\
  .map(lambda (userid,values): values)
```

**2. Explorer the persistence Level**

```
> accountHits.take(2)
[(u'Pope,Ricky', 4), (u'Allen,Garrett', 2)]

> accountHits.filter(lambda (firstlast, hitcount): hitcount > 5).count()
5872

# Because persist is not Action, at this point accountHits RDD does not saved on the memory
> accountHits.persist()

# accountHits RDD comprised with 23 partitions
> accountHits.toDebugString()
'(23) PythonRDD[36] at RDD at PythonRDD.scala:43 []\n |   MapPartitionsRDD[34] at mapPartitions at PythonRDD.scala:374 []\n |   ShuffledRDD[33] at partitionBy at NativeMethodAccessorImpl.java:-2 []\n +-(23) PairwiseRDD[32] at join at <ipython-input-11-19ae9341818a>:12 []\n    |   PythonRDD[31] at join at <ipython-input-11-19ae9341818a>:12 []\n    |   UnionRDD[30] at union at NativeMethodAccessorImpl.java:-2 []\n    |   PythonRDD[28] at RDD at PythonRDD.scala:43 []\n    |   /loudacre/accounts/* MapPartitionsRDD[27] at textFile at NativeMethodAccessorImpl.java:-2 []\n    |   /loudacre/accounts/* HadoopRDD[26] at textFile at NativeMethodAccessorImpl.java:-2 []\n    |   PythonRDD[29] at RDD at PythonRDD.scala:43 []\n    |   MapPartitionsRDD[25] at mapPartitions at PythonRDD.scala:374 []\n    |   ShuffledRDD[24] at partitionBy at NativeMethodAccessorImpl.java:-2 []\n    +-(18) PairwiseRDD[23] at reduceByKey at <ipython-input-11-19ae9341818a>:6 []\n       |   PythonRDD[22] at reduceByKey at <ipython-input-11-19ae9341818a>:6 []\n       |   /loudacre/weblogs/*2.log MapPartitionsRDD[21] at textFile at NativeMethodAccessorImpl.java:-2 []\n       |   /loudacre/weblogs/*2.log HadoopRDD[20] at textFile at NativeMethodAccessorImpl.java:-2 []'

# count, which is an Action operation is submitted, then persist works
> accountHits.filter(lambda (firstlast, hitcount): hitcount > 5).count()

# You can see the CachedPartitions and the momorySize
> accountHits.toDebugString()
'(23) PythonRDD[36] at RDD at PythonRDD.scala:43 [Memory Serialized 1x Replicated]\n |        CachedPartitions: 23; MemorySize: 311.2 KB; ExternalBlockStoreSize: 0.0 B; DiskSize: 0.0 B\n |   MapPartitionsRDD[34] at mapPartitions at PythonRDD.scala:374 [Memory Serialized 1x Replicated]\n |   ShuffledRDD[33] at partitionBy at NativeMethodAccessorImpl.java:-2 [Memory Serialized 1x Replicated]\n +-(23) PairwiseRDD[32] at join at <ipython-input-11-19ae9341818a>:12 [Memory Serialized 1x Replicated]\n    |   PythonRDD[31] at join at <ipython-input-11-19ae9341818a>:12 [Memory Serialized 1x Replicated]\n    |   UnionRDD[30] at union at NativeMethodAccessorImpl.java:-2 [Memory Serialized 1x Replicated]\n    |   PythonRDD[28] at RDD at PythonRDD.scala:43 [Memory Serialized 1x Replicated]\n    |   /loudacre/accounts/* MapPartitionsRDD[27] at textFile at NativeMethodAccessorImpl.java:-2 [Memory Serialized 1x Replicated]\n    |   /loudacre/accounts/* HadoopRDD[26] at textFile at NativeMethodAccessorImpl.java:-2 [Memory Serialized 1x Replicated]\n    |   PythonRDD[29] at RDD at PythonRDD.scala:43 [Memory Serialized 1x Replicated]\n    |   MapPartitionsRDD[25] at mapPartitions at PythonRDD.scala:374 [Memory Serialized 1x Replicated]\n    |   ShuffledRDD[24] at partitionBy at NativeMethodAccessorImpl.java:-2 [Memory Serialized 1x Replicated]\n    +-(18) PairwiseRDD[23] at reduceByKey at <ipython-input-11-19ae9341818a>:6 [Memory Serialized 1x Replicated]\n       |   PythonRDD[22] at reduceByKey at <ipython-input-11-19ae9341818a>:6 [Memory Serialized 1x Replicated]\n       |   /loudacre/weblogs/*2.log MapPartitionsRDD[21] at textFile at NativeMethodAccessorImpl.java:-2 [Memory Serialized 1x Replicated]\n       |   /loudacre/weblogs/*2.log HadoopRDD[20] at textFile at NativeMethodAccessorImpl.java:-2 [Memory Serialized 1x Replicated]'

# If you want to change the Persistence Level, you have to unpersist() first
> accountHits.unpersist()
> accountHits.toDebugString()
'(23) PythonRDD[36] at RDD at PythonRDD.scala:43 []\n |   MapPartitionsRDD[34] at mapPartitions at PythonRDD.scala:374 []\n |   ShuffledRDD[33] at partitionBy at NativeMethodAccessorImpl.java:-2 []\n +-(23) PairwiseRDD[32] at join at <ipython-input-11-19ae9341818a>:12 []\n    |   PythonRDD[31] at join at <ipython-input-11-19ae9341818a>:12 []\n    |   UnionRDD[30] at union at NativeMethodAccessorImpl.java:-2 []\n    |   PythonRDD[28] at RDD at PythonRDD.scala:43 []\n    |   /loudacre/accounts/* MapPartitionsRDD[27] at textFile at NativeMethodAccessorImpl.java:-2 []\n    |   /loudacre/accounts/* HadoopRDD[26] at textFile at NativeMethodAccessorImpl.java:-2 []\n    |   PythonRDD[29] at RDD at PythonRDD.scala:43 []\n    |   MapPartitionsRDD[25] at mapPartitions at PythonRDD.scala:374 []\n    |   ShuffledRDD[24] at partitionBy at NativeMethodAccessorImpl.java:-2 []\n    +-(18) PairwiseRDD[23] at reduceByKey at <ipython-input-11-19ae9341818a>:6 []\n       |   PythonRDD[22] at reduceByKey at <ipython-input-11-19ae9341818a>:6 []\n       |   /loudacre/weblogs/*2.log MapPartitionsRDD[21] at textFile at NativeMethodAccessorImpl.java:-2 []\n       |   /loudacre/weblogs/*2.log HadoopRDD[20] at textFile at NativeMethodAccessorImpl.java:-2 []'

# Change the Persistence Level to save RDD on DISK (Default : Memory only)
> accountHits.persist(StorageLevel.DISK_ONLY)
> accountHits.filter(lambda (firstlast, hitcount): hitcount > 5).count()
> accountHits.toDebugString()
'(23) PythonRDD[36] at RDD at PythonRDD.scala:43 [Disk Serialized 1x Replicated]\n |        CachedPartitions: 23; MemorySize: 0.0 B; ExternalBlockStoreSize: 0.0 B; DiskSize: 311.2 KB\n |   MapPartitionsRDD[34] at mapPartitions at PythonRDD.scala:374 [Disk Serialized 1x Replicated]\n |   ShuffledRDD[33] at partitionBy at NativeMethodAccessorImpl.java:-2 [Disk Serialized 1x Replicated]\n +-(23) PairwiseRDD[32] at join at <ipython-input-11-19ae9341818a>:12 [Disk Serialized 1x Replicated]\n    |   PythonRDD[31] at join at <ipython-input-11-19ae9341818a>:12 [Disk Serialized 1x Replicated]\n    |   UnionRDD[30] at union at NativeMethodAccessorImpl.java:-2 [Disk Serialized 1x Replicated]\n    |   PythonRDD[28] at RDD at PythonRDD.scala:43 [Disk Serialized 1x Replicated]\n    |   /loudacre/accounts/* MapPartitionsRDD[27] at textFile at NativeMethodAccessorImpl.java:-2 [Disk Serialized 1x Replicated]\n    |   /loudacre/accounts/* HadoopRDD[26] at textFile at NativeMethodAccessorImpl.java:-2 [Disk Serialized 1x Replicated]\n    |   PythonRDD[29] at RDD at PythonRDD.scala:43 [Disk Serialized 1x Replicated]\n    |   MapPartitionsRDD[25] at mapPartitions at PythonRDD.scala:374 [Disk Serialized 1x Replicated]\n    |   ShuffledRDD[24] at partitionBy at NativeMethodAccessorImpl.java:-2 [Disk Serialized 1x Replicated]\n    +-(18) PairwiseRDD[23] at reduceByKey at <ipython-input-11-19ae9341818a>:6 [Disk Serialized 1x Replicated]\n       |   PythonRDD[22] at reduceByKey at <ipython-input-11-19ae9341818a>:6 [Disk Serialized 1x Replicated]\n       |   /loudacre/weblogs/*2.log MapPartitionsRDD[21] at textFile at NativeMethodAccessorImpl.java:-2 [Disk Serialized 1x Replicated]\n       |   /loudacre/weblogs/*2.log HadoopRDD[20] at textFile at NativeMethodAccessorImpl.java:-2 [Disk Serialized 1x Replicated]'
```

<Lab8. Use Apache Spark SQL for ETL>
-------------------------

**1. Make DataFrame from dataSource**

```
> sqlContext
<pyspark.sql.context.HiveContext at 0x7fd9c0c6d050>

> webpageDF = sqlContext.read.load("/loudacre/webpage")
> webpageDF.printSchema()
root
 |-- web_page_num: integer (nullable = true)
 |-- web_page_file_name: string (nullable = true)
 |-- associated_files: string (nullable = true)

> webpageDF.show(5)
+------------+--------------------+--------------------+
|web_page_num|  web_page_file_name|    associated_files|
+------------+--------------------+--------------------+
|           1|sorrento_f00l_sal...|theme1.css,code.j...|
|           2|titanic_2100_sale...|theme3.css,code.j...|
|           3|meetoo_3.0_sales....|theme3.css,code.j...|
|           4|meetoo_3.1_sales....|theme.css,code.js...|
|           5| ifruit_1_sales.html|theme1.css,code.j...|
+------------+--------------------+--------------------+
only showing top 5 rows

> assocFilesDF = webpageDF.select(webpageDF.web_page_num, webpageDF.associated_files)
> assocFilesDF.printSchema()
root
 |-- web_page_num: integer (nullable = true)
 |-- associated_files: string (nullable = true)

> assocFilesDF.show(5)
+------------+--------------------+
|web_page_num|    associated_files|
+------------+--------------------+
|           1|theme1.css,code.j...|
|           2|theme3.css,code.j...|
|           3|theme3.css,code.j...|
|           4|theme.css,code.js...|
|           5|theme1.css,code.j...|
+------------+--------------------+
only showing top 5 rows
```

**2. Make RDD with DataFrame**

```
> aFilesRDD = assocFilesDF.map(lambda row: (row.web_page_num, row.associated_files))
> aFilesRDD2 = aFilesRDD.flatMapValues(lambda filestring: filestring.split(','))
> aFilesRDD2.take(2)
[(1, u'theme1.css'), (1, u'code.js')]
```

**3. Make DataFrame with RDD**

```
> aFileDF = sqlContext.createDataFrame(aFilesRDD2, assocFilesDF.schema)
> aFileDF.printSchema()
root
 |-- web_page_num: integer (nullable = true)
 |-- associated_files: string (nullable = true)

> aFileDF.show(5)
+------------+-----------------+
|web_page_num| associated_files|
+------------+-----------------+
|           1|       theme1.css|
|           1|          code.js|
|           1|sorrento_f00l.jpg|
|           2|       theme3.css|
|           2|          code.js|
+------------+-----------------+
only showing top 5 rows

> finalDF = aFileDF.withColumnRenamed('associated_files', 'associated_file')
> finalDF.printSchema()
root
 |-- web_page_num: integer (nullable = true)
 |-- associated_file: string (nullable = true)
 
> finalDF.show(5)
+------------+-----------------+
|web_page_num|  associated_file|
+------------+-----------------+
|           1|       theme1.css|
|           1|          code.js|
|           1|sorrento_f00l.jpg|
|           2|       theme3.css|
|           2|          code.js|
+------------+-----------------+
only showing top 5 rows
```

**4. Write(Save) DataFrame to File-system**

```
> finalDF.write.mode("overwrite").save("/loudacre/webpage_files")
```


<Lab9. Write an Apache Spark Streaming Application>
-------------------------

**1. Writing a Spark Streaming Application**

/home/training/training_materials/devsh/exercises/spark-streaming/stubs-python/StreamingLogs.py
```
mport sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Given an RDD of KB requests, print out the count of elements
def printRDDcount(rdd): print "Number of KB requests: "+str(rdd.count())

def print5(r, t):
    print "5 data in the Input Stream @" ,t
    for temp in r.take(5):
        print temp


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: StreamingLogs.py <hostname> <port>"
        sys.exit(-1)

    # get hostname and port of data source from application arguments
    hostname = sys.argv[1]
    port = int(sys.argv[2])

    # Create a new SparkContext
    sc = SparkContext()

    # Set log level to ERROR to avoid distracting extra output
    sc.setLogLevel("ERROR")

   # TODO
    ssc = StreamingContext(sc,1)

    mystream = ssc.socketTextStream(hostname, port)

    mystream2 = mystream.filter(lambda line: "KBDOC" in line)

    mystream2.pprint(5)

    mystream2.foreachRDD(lambda t,r: printRDDcount(r))

    mystream2.saveAsTextFiles("/loudacre/streamlog/kblogs")

    ssc.start()
    ssc.awaitTermination()
```

**2. Web log Generator start**

```
> cd $DEVSH/exercises/spark-streaming
> python streamtest.py localhost 1234 20 $DEVDATA/weblogs/*
```

**3. Testing the Application**

```
> cd $DEVSH/exercises/spark-streaming
> spark-submit --master 'local[2]' stubs-python/StreamingLogs.py localhost 1234
```
