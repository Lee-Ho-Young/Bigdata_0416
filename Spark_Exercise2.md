**통신QM Unit 이호영 선임(09340)**

<Lab1. Write an Apache Spark Streaming Application>
-------------------------

**1. Code in python**

```
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: StreamingLogsMB.py <hostname> <port>"
        sys.exit(-1)

    # get hostname and port of data source from application arguments
    hostname = sys.argv[1]
    port = int(sys.argv[2])

    # Create a new SparkContext
    sc = SparkContext()

    # Set log level to ERROR to avoid distracting extra output
    sc.setLogLevel("ERROR")

    # Create and configure a new Streaming Context
    # with a 1 second batch duration
    ssc = StreamingContext(sc,1)

    # Create a DStream of log data from the server and port specified
    logs = ssc.socketTextStream(hostname,port)

    # TODO

    # Window
    logs.pprint()
    logs.window(4,2).pprint()
    #logs.countByWindow(5,2).pprint()

    # Checkpoint
    ssc.checkpoint("logcheckpt")


    ssc.start()
    ssc.awaitTermination()

```

**2. Starting Socket Streaming**

```
[training@localhost spark-streaming-multi]$ python streamtest.py localhost 12344 1 $DEVDATA/weblogs/*
```

```
[training@localhost spark-streaming-multi]$ spark-submit --master 'local[2]' stubs-python/StreamingLogsMB.py localhost 12344
```


**3. Review the outcomes**

```
-------------------------------------------
Time: 2019-04-17 17:29:50
-------------------------------------------
3.94.78.5 - 69827 [15/Sep/2013:23:58:36 +0100] "GET /KBDOC-00033.html HTTP/1.0" 200 14417 "http://www.loudacre.com"  "Loudacre Mobile Browser iFruit 1"

-------------------------------------------
Time: 2019-04-17 17:29:50
-------------------------------------------
3.94.78.5 - 69827 [15/Sep/2013:23:58:36 +0100] "GET /KBDOC-00033.html HTTP/1.0" 200 14417 "http://www.loudacre.com"  "Loudacre Mobile Browser iFruit 1"

-------------------------------------------
Time: 2019-04-17 17:29:51
-------------------------------------------
3.94.78.5 - 69827 [15/Sep/2013:23:58:36 +0100] "GET /theme.css HTTP/1.0" 200 3576 "http://www.loudacre.com"  "Loudacre Mobile Browser iFruit 1"

-------------------------------------------
Time: 2019-04-17 17:29:52
-------------------------------------------
19.38.140.62 - 21475 [15/Sep/2013:23:58:34 +0100] "GET /KBDOC-00277.html HTTP/1.0" 200 15517 "http://www.loudacre.com"  "Loudacre Mobile Browser Ronin S1"

-------------------------------------------
Time: 2019-04-17 17:29:52
-------------------------------------------
3.94.78.5 - 69827 [15/Sep/2013:23:58:36 +0100] "GET /KBDOC-00033.html HTTP/1.0" 200 14417 "http://www.loudacre.com"  "Loudacre Mobile Browser iFruit 1"
3.94.78.5 - 69827 [15/Sep/2013:23:58:36 +0100] "GET /theme.css HTTP/1.0" 200 3576 "http://www.loudacre.com"  "Loudacre Mobile Browser iFruit 1"
19.38.140.62 - 21475 [15/Sep/2013:23:58:34 +0100] "GET /KBDOC-00277.html HTTP/1.0" 200 15517 "http://www.loudacre.com"  "Loudacre Mobile Browser Ronin S1"

-------------------------------------------
Time: 2019-04-17 17:29:53
-------------------------------------------
19.38.140.62 - 21475 [15/Sep/2013:23:58:34 +0100] "GET /theme.css HTTP/1.0" 200 13353 "http://www.loudacre.com"  "Loudacre Mobile Browser Ronin S1"

-------------------------------------------
Time: 2019-04-17 17:29:54
-------------------------------------------
129.133.56.105 - 2489 [15/Sep/2013:23:58:34 +0100] "GET /KBDOC-00033.html HTTP/1.0" 200 10590 "http://www.loudacre.com"  "Loudacre Mobile Browser Sorrento F00L"

-------------------------------------------
Time: 2019-04-17 17:29:54
-------------------------------------------
3.94.78.5 - 69827 [15/Sep/2013:23:58:36 +0100] "GET /theme.css HTTP/1.0" 200 3576 "http://www.loudacre.com"  "Loudacre Mobile Browser iFruit 1"
19.38.140.62 - 21475 [15/Sep/2013:23:58:34 +0100] "GET /KBDOC-00277.html HTTP/1.0" 200 15517 "http://www.loudacre.com"  "Loudacre Mobile Browser Ronin S1"
19.38.140.62 - 21475 [15/Sep/2013:23:58:34 +0100] "GET /theme.css HTTP/1.0" 200 13353 "http://www.loudacre.com"  "Loudacre Mobile Browser Ronin S1"
129.133.56.105 - 2489 [15/Sep/2013:23:58:34 +0100] "GET /KBDOC-00033.html HTTP/1.0" 200 10590 "http://www.loudacre.com"  "Loudacre Mobile Browser Sorrento F00L"

-------------------------------------------
Time: 2019-04-17 17:29:55
-------------------------------------------
129.133.56.105 - 2489 [15/Sep/2013:23:58:34 +0100] "GET /theme.css HTTP/1.0" 200 12295 "http://www.loudacre.com"  "Loudacre Mobile Browser Sorrento F00L"

-------------------------------------------
Time: 2019-04-17 17:29:56
-------------------------------------------
217.150.149.167 - 4712 [15/Sep/2013:23:56:06 +0100] "GET /ronin_s4_sales.html HTTP/1.0" 200 845 "http://www.loudacre.com"  "Loudacre Mobile Browser MeeToo 1.0"

-------------------------------------------
Time: 2019-04-17 17:29:56
-------------------------------------------
19.38.140.62 - 21475 [15/Sep/2013:23:58:34 +0100] "GET /theme.css HTTP/1.0" 200 13353 "http://www.loudacre.com"  "Loudacre Mobile Browser Ronin S1"
129.133.56.105 - 2489 [15/Sep/2013:23:58:34 +0100] "GET /KBDOC-00033.html HTTP/1.0" 200 10590 "http://www.loudacre.com"  "Loudacre Mobile Browser Sorrento F00L"
129.133.56.105 - 2489 [15/Sep/2013:23:58:34 +0100] "GET /theme.css HTTP/1.0" 200 12295 "http://www.loudacre.com"  "Loudacre Mobile Browser Sorrento F00L"
217.150.149.167 - 4712 [15/Sep/2013:23:56:06 +0100] "GET /ronin_s4_sales.html HTTP/1.0" 200 845 "http://www.loudacre.com"  "Loudacre Mobile Browser MeeToo 1.0"

-------------------------------------------
Time: 2019-04-17 17:29:57
-------------------------------------------
217.150.149.167 - 4712 [15/Sep/2013:23:56:06 +0100] "GET /theme.css HTTP/1.0" 200 738 "http://www.loudacre.com"  "Loudacre Mobile Browser MeeToo 1.0"

-------------------------------------------
Time: 2019-04-17 17:29:58
-------------------------------------------
217.150.149.167 - 4712 [15/Sep/2013:23:56:06 +0100] "GET /code.js HTTP/1.0" 200 938 "http://www.loudacre.com"  "Loudacre Mobile Browser MeeToo 1.0"

-------------------------------------------
Time: 2019-04-17 17:29:58
-------------------------------------------
129.133.56.105 - 2489 [15/Sep/2013:23:58:34 +0100] "GET /theme.css HTTP/1.0" 200 12295 "http://www.loudacre.com"  "Loudacre Mobile Browser Sorrento F00L"
217.150.149.167 - 4712 [15/Sep/2013:23:56:06 +0100] "GET /ronin_s4_sales.html HTTP/1.0" 200 845 "http://www.loudacre.com"  "Loudacre Mobile Browser MeeToo 1.0"
217.150.149.167 - 4712 [15/Sep/2013:23:56:06 +0100] "GET /theme.css HTTP/1.0" 200 738 "http://www.loudacre.com"  "Loudacre Mobile Browser MeeToo 1.0"
217.150.149.167 - 4712 [15/Sep/2013:23:56:06 +0100] "GET /code.js HTTP/1.0" 200 938 "http://www.loudacre.com"  "Loudacre Mobile Browser MeeToo 1.0"

-------------------------------------------
Time: 2019-04-17 17:29:59
-------------------------------------------
217.150.149.167 - 4712 [15/Sep/2013:23:56:06 +0100] "GET /ronin_s4.jpg HTTP/1.0" 200 5552 "http://www.loudacre.com"  "Loudacre Mobile Browser MeeToo 1.0"

-------------------------------------------
Time: 2019-04-17 17:30:00
-------------------------------------------
209.151.12.34 - 45922 [15/Sep/2013:23:55:09 +0100] "GET /KBDOC-00259.html HTTP/1.0" 200 19362 "http://www.loudacre.com"  "Loudacre Mobile Browser Sorrento F11L"

-------------------------------------------
Time: 2019-04-17 17:30:00
-------------------------------------------
217.150.149.167 - 4712 [15/Sep/2013:23:56:06 +0100] "GET /theme.css HTTP/1.0" 200 738 "http://www.loudacre.com"  "Loudacre Mobile Browser MeeToo 1.0"
217.150.149.167 - 4712 [15/Sep/2013:23:56:06 +0100] "GET /code.js HTTP/1.0" 200 938 "http://www.loudacre.com"  "Loudacre Mobile Browser MeeToo 1.0"
217.150.149.167 - 4712 [15/Sep/2013:23:56:06 +0100] "GET /ronin_s4.jpg HTTP/1.0" 200 5552 "http://www.loudacre.com"  "Loudacre Mobile Browser MeeToo 1.0"
209.151.12.34 - 45922 [15/Sep/2013:23:55:09 +0100] "GET /KBDOC-00259.html HTTP/1.0" 200 19362 "http://www.loudacre.com"  "Loudacre Mobile Browser Sorrento F11L"

-------------------------------------------
Time: 2019-04-17 17:30:01
-------------------------------------------
209.151.12.34 - 45922 [15/Sep/2013:23:55:09 +0100] "GET /theme.css HTTP/1.0" 200 17795 "http://www.loudacre.com"  "Loudacre Mobile Browser Sorrento F11L"

-------------------------------------------
Time: 2019-04-17 17:30:02
-------------------------------------------
184.97.84.245 - 144 [15/Sep/2013:23:54:55 +0100] "GET /KBDOC-00052.html HTTP/1.0" 200 12499 "http://www.loudacre.com"  "Loudacre CSR Browser"

-------------------------------------------
Time: 2019-04-17 17:30:02
-------------------------------------------
217.150.149.167 - 4712 [15/Sep/2013:23:56:06 +0100] "GET /ronin_s4.jpg HTTP/1.0" 200 5552 "http://www.loudacre.com"  "Loudacre Mobile Browser MeeToo 1.0"
209.151.12.34 - 45922 [15/Sep/2013:23:55:09 +0100] "GET /KBDOC-00259.html HTTP/1.0" 200 19362 "http://www.loudacre.com"  "Loudacre Mobile Browser Sorrento F11L"
209.151.12.34 - 45922 [15/Sep/2013:23:55:09 +0100] "GET /theme.css HTTP/1.0" 200 17795 "http://www.loudacre.com"  "Loudacre Mobile Browser Sorrento F11L"
184.97.84.245 - 144 [15/Sep/2013:23:54:55 +0100] "GET /KBDOC-00052.html HTTP/1.0" 200 12499 "http://www.loudacre.com"  "Loudacre CSR Browser"

-------------------------------------------
Time: 2019-04-17 17:30:03
-------------------------------------------
184.97.84.245 - 144 [15/Sep/2013:23:54:55 +0100] "GET /theme.css HTTP/1.0" 200 4979 "http://www.loudacre.com"  "Loudacre CSR Browser"

-------------------------------------------
Time: 2019-04-17 17:30:04
-------------------------------------------
233.60.251.2 - 33908 [15/Sep/2013:23:51:43 +0100] "GET /KBDOC-00292.html HTTP/1.0" 200 4779 "http://www.loudacre.com"  "Loudacre Mobile Browser Ronin S2"

-------------------------------------------
Time: 2019-04-17 17:30:04
-------------------------------------------
209.151.12.34 - 45922 [15/Sep/2013:23:55:09 +0100] "GET /theme.css HTTP/1.0" 200 17795 "http://www.loudacre.com"  "Loudacre Mobile Browser Sorrento F11L"
184.97.84.245 - 144 [15/Sep/2013:23:54:55 +0100] "GET /KBDOC-00052.html HTTP/1.0" 200 12499 "http://www.loudacre.com"  "Loudacre CSR Browser"
184.97.84.245 - 144 [15/Sep/2013:23:54:55 +0100] "GET /theme.css HTTP/1.0" 200 4979 "http://www.loudacre.com"  "Loudacre CSR Browser"
233.60.251.2 - 33908 [15/Sep/2013:23:51:43 +0100] "GET /KBDOC-00292.html HTTP/1.0" 200 4779 "http://www.loudacre.com"  "Loudacre Mobile Browser Ronin S2"

-------------------------------------------
Time: 2019-04-17 17:30:05
-------------------------------------------
233.60.251.2 - 33908 [15/Sep/2013:23:51:43 +0100] "GET /theme.css HTTP/1.0" 200 15871 "http://www.loudacre.com"  "Loudacre Mobile Browser Ronin S2"

-------------------------------------------
Time: 2019-04-17 17:30:06
-------------------------------------------
160.134.139.204 - 51340 [15/Sep/2013:23:50:11 +0100] "GET /KBDOC-00016.html HTTP/1.0" 200 1156 "http://www.loudacre.com"  "Loudacre Mobile Browser MeeToo 3.0"

-------------------------------------------
Time: 2019-04-17 17:30:06
-------------------------------------------
184.97.84.245 - 144 [15/Sep/2013:23:54:55 +0100] "GET /theme.css HTTP/1.0" 200 4979 "http://www.loudacre.com"  "Loudacre CSR Browser"
233.60.251.2 - 33908 [15/Sep/2013:23:51:43 +0100] "GET /KBDOC-00292.html HTTP/1.0" 200 4779 "http://www.loudacre.com"  "Loudacre Mobile Browser Ronin S2"
233.60.251.2 - 33908 [15/Sep/2013:23:51:43 +0100] "GET /theme.css HTTP/1.0" 200 15871 "http://www.loudacre.com"  "Loudacre Mobile Browser Ronin S2"
160.134.139.204 - 51340 [15/Sep/2013:23:50:11 +0100] "GET /KBDOC-00016.html HTTP/1.0" 200 1156 "http://www.loudacre.com"  "Loudacre Mobile Browser MeeToo 3.0"
```







