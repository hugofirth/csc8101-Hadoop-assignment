##MapReduce programming: using hadoop to calculate the internal PageRank of wikipedia articles.

----

This coursework introduces you to the **MapReduce** model of programming and data manipulation. It will provide limited practical experience of analysing a real data source: [wikipedia](wikipedia.org).

###Data:

For the purposes of this assignment you have been provided with a body of dutch wikipedia articles, stored as a single `.xml` document.

The document is formatted as follows:

``` xml
<mediawiki>
    <page>...</page>
    <page>
        <title>Foo</title>
        <text>Lorem ipsum dolor sit amet...</text>
    </page>
    <page>
        <title>Bar</title>
        <text>Lorem ipsum dolor sit [[Foo|amet]]...</text>
    </page>
        <page>
        <title>Baz</title>
        <text>[[Foo]] [[Bar]] not Lipsum...</text>
    </page>
    <page>...<page>
</mediawiki>
```

**Note:** Some unimportant tags and attributes are ommitted for brevity. If you wish to see the full structure of the xml document, see `example-input.xml`.

Each article is represented by a `<page></page>` element. Links to other articles are represented in the form `[[PageTitle]]`. Link text is normally just the title of the linked article, however links may include an optional pipe symbole `|` followed by some alternative text.

###What:

Using the [**hadoop**](http://hadoop.apache.org/) MapReduce framework, your task is to calculate the [**PageRank**](http://en.wikipedia.org/wiki/PageRank) of each wikipedia article and produce an ordered list of article titles, along with their ranks.

From the lecture materials, you should already be familiar with the MapReduce model of programming and how it relates to PageRank. 

The java program with which you have been provided consists of three hadoop jobs, `parse`, `calculate` and `rank`, pictured below: 

![Jobs](https://dl.dropboxusercontent.com/u/6430/PageRank.png)

#####Parse
The `parse` job is provided for you, and merely captures the outgoing links from each article in the dataset.

#####Calculate
The `calculate` job should calculate Pagerank, or _PR(A)_ using a simple iterative algorithm as an alternative interpretation of the matrix based techniques you have seen in the lecture.

Quoting from the [original Google paper](http://ilpubs.stanford.edu:8090/422/1/1999-66.pdf), PageRank is defined like this:

>We assume page A has pages T1...Tn which point to it (i.e., are citations). The parameter d is a damping factor which can be set between 0 and 1. We usually set d to 0.85. There are more details about d in the next section. Also C(A) is defined as the number of links going out of page A. The PageRank of a page A is given as follows:
>
>PR(A) = (1-d) + d (PR(T1)/C(T1) + ... + PR(Tn)/C(Tn))
>
>Note that the PageRanks form a probability distribution over web pages, so the sum of all web pages' PageRanks will be one.
>
>PageRank or PR(A) can be calculated using a simple iterative algorithm, and corresponds to the principal eigenvector of the normalized link matrix of the web.

but that’s not too helpful so let’s break it down into sections.

* **PR(Tn)** - Each page has a notion of its own self-importance. That’s “PR(T1)” for the first page in the web all the way up to “PR(Tn)” for the last page
* **C(Tn)** - Each page spreads its vote out evenly amongst all of it’s outgoing links. The count, or number, of outgoing links for page 1 is “C(T1)”, “C(Tn)” for page n, and so on for all pages.
* **PR(Tn)/C(Tn)** - so if our page (page A) has a backlink from page “n” the share of the vote page A will get is “PR(Tn)/C(Tn)”
* **d(...** - All these fractions of votes are added together but, to stop the other pages having too much influence, this total vote is “damped down” by multiplying it by 0.85 (the factor “d”)
* **(1 - d)** - The (1 – d) bit at the beginning is a bit of probability math magic so the “sum of all web pages' PageRanks will be one”: it adds in the bit lost by the d(.... It also means that if a page has no links to it (no backlinks) even then it will still get a small PR of 0.15 (i.e. 1 – 0.85). (Aside: the Google paper says “the sum of all pages” but they mean the “the normalised sum” – otherwise known as “the average” to you and me.

#####Rank
The `rank` job should associate each page with its final PageRank value.

###How:

You must fill out the following blank methods in the provided java project:

* `RankCalculateMapper#map(LongWritable key, Text value, Context context)`
* `RankCalculateReducer#reduce(Text page, Iterable<Text> values, Context context)`
* `RankSortMapper#map(LongWritable key, Text value, Context context)`

**Step 0**. Clone this repository with the following command:

`git clone https://github.com/hugofirth/csc8101-Hadoop-assignment.git`

**Step 1**. Code your solution...

**Step 2**. Build your solution by executing the command

`mvn clean package`

from the root of your project.

**Step 3**. Once the build has finished running, transfer two files to your VM (**make sure it is running**):

`scp -i CSC8101-2014-15-student.pem target/csc8101-hadoop-assignment-1.0-SNAPSHOT-jar-with-dependencies.jar ubuntu@<your-VM-ip>:/home/ubunutu`

`scp -i CSC8101-2014-15-student.pem example-input.xml ubuntu@<your-VM-ip>:/home/ubunutu/data/hadoop_pagerank`

**Step 4**. From your VM, as the user hduser, start hadoop as described [here](https://docs.google.com/document/d/1BVvOzFvUqhyxG76jG7GVX_GgHkc-eiaaNJMnhtlmdNM)

**Step 5**. From your VM, as the user hduser, copy the `example-input.xml` into hdfs with the following command:

`hadoop fs -copyFromLocal /home/ubuntu/data/hadoop_pagerank/example-wiki.xml /user/hduser/input/HadoopPageRank/wiki`

**Step 6**. Make sure that your the job's `output` directory in hdfs is clear:

`hadoop fs -rmr /user/hduser/output/HadoopPageRank/*`

**Step 7**. Run the hadoop jar with the following command:

`hadoop jar csc8101-hadoop-assignment-1.0-SNAPSHOT-jar-with-dependencies.jar uk.ac.ncl.cs.csc8101.hadoop.PageRankJob`

**Step 8**. When the hadoop execution has completed, you may get your results out from hdfs with the following command:

`hadoop fs -copyToLocal /user/hduser/output/HadoopPageRank/result/part-r-00000 .`


**Step 9**. When your code seems to be working satisfactorily, you should unzip the large xml file with the following command:

`bzip2 -dk nlwiki-latest-pages-articles.xml.bz2`

**Note**: This may take some time.

**Step 10**. Copy the unzipped xml document to the `/user/hduser/input/HadoopPageRank/wiki` directory in hdfs as in step **5**.

**Step 11**. Make sure the `/user/hduser/output/HadoopPageRank` directory in hdfs is clear as in step **6**.

**Step 12**. Re-run the hadoop job as in step **7**.

###Submission

When you are happy with the output of your code, you should submit the following to NESS:

* Your project `src` directory, zipped up.
* The output of the command `tail -n 100 part-r-0000` as a text file.

###Frequently asked questions

I will maintain a list of frequently asked questions below. If you have a question for a demonstrator, please check here first.

**Question**: If the `calculate` job is iterative and the output from the reduce step forms the input to the next map step, how should you preserve pages which have no incoming links?

> **Answer**: You should produce an additional key-value pair from the map step which denotes the existence of a page and records its outgoing links. The job diagram has been updated to reflect this. 
> **Note**: Choosing a control character for the start of these values makes them easier to differentiate from those pairs which represent links.

**Question**: I am on Windows and PuTTy for ssh. What should I do about the `.pem` file?

> **Answer**: The process is slightly different on windows, involving converting the key to a different format. The process is described [here](https://linuxacademy.com/blog/linux/connect-to-amazon-ec2-using-putty-private-key-on-windows/).
> **Note**: Where the tutorial says to use the user `root`, you must instead use the user `ubuntu`.

> **Alternative Answer**: You may use PuTTy to ssh into `linux.cs.ncl.ac.uk`, using your university credentials, and perform the coursework there. 
> **Note**: This will require placing the `.pem` file and project sources in your network home folder.

**Question**: How do I output values from the map and reduce methods?

> **Answer**: You should use the `Context` object ([documentation](https://hadoop.apache.org/docs/r1.2.1/api/org/apache/hadoop/mapreduce/TaskInputOutputContext.html)). Specifically the `write([key], [value])` method. A Context is the main form of communication between steps in the Hadoop API.


