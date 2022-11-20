
# Update log
* Nov 20th: Changed description to use 'q3' naming instead of 'graph' for the mentions graph
* Nov 20th: Use FLOAT instead of Double
* Nov 12th: Added new images to assist with setup
* Nov 7th: Updated role assignment for service account (should be Owner instead of BigQuery Data Owner)
* Nov 7th: Created document guiding how to update role for service accounts that already exists ([here](https://github.com/w4111/project2-f22-template/blob/main/Adding%20role%20to%20your%20existing%20key.pdf))


# Project 2: Graph Analysis

Released: 11/01  
Due: 11/29 23:55  
Value: 5% of your grade  
A maximum of **3 Late Days** can be used on this project  
Max team of 2  


Many graph analysis compute network centrality, density, shortest paths, and other path-based statistics about a graph. It may seem that writing a one-off Python script is a good way to perform this analysis, but it turns out that SQL is pretty great at doing this type of analysis!

You will use [DBT](https://www.getdbt.com/product/what-is-dbt/) for this project, alongside with BigQuery (Google's data warehouse service)! This is a great opportunity to use a tool that has some popularity in companies for data-related tasks.

## Setup

Read the [Project 2 Setup document](https://github.com/w4111/project2-f22-template/blob/main/Project%202%20setup.pdf) to set up BigQuery, DBT and a git repository

**Important** You will submit the sql files that are on the models folder. **DO NOT CHANGE THE FILES NAMES**
## Refresher
You will write queries to answer the following questions about the dataset.

In the simple case, graphs have the following schema:
```SQL
nodes(id int primary key, )
    edges(
      src int NOT NULL references nodes(id),
      dst int NOT NULL references nodes(id), 
    )
```
Recall that in graph analysis, you are interested in finding neighbors of nodes or paths between nodes. Following an edge in the graph corresponds to a JOIN. For example, the following finds all neighbors of node id 2:
```SQL
    SELECT dst FROM edges WHERE src = 2;
```
Similarly, if we have a table goodnodes that contains IDs of nodes that we are interested in, the following query finds neighbors of these good nodes:
```SQL
    SELECT dst FROM edges, goodnodes WHERE edges.src = goodnodes.id;
```
## Question 1 (3 points)
Find the id and text of Tweets whose text contains "MAGA" and "Trump" (both case-insensitive).

For example:

"#VoteTrump and lets all help #Trump #maga" is a match
"#Trump This was our moment. Together, we will M AGA!" is not a match
"Trump shows in the New York Times Magazine " is a match
Your answer should be a single query. You need to run it and get the output containing the columns (note that the column order matters for the whole project!!!):

* `id` (tweetid of the tweets)
* `text` (tweet_text of the tweets)

You should write your model on file q1.sql

## Question 2 (3 points)
"MakeAmericaGreatAgain" (often abbreviated as MAGA) is a campaign slogan used in American politics that was popularized by Donald Trump in his successful 2016 presidential campaign.

Let's find out Top 5 months that get the maximum mention of this slogan "MAGA"(case-insensitive) in tweet_text.

The months in different years are considered different.

Your answer should be a single query. You need to run it and get the output containing the columns:

* `year` (a number of four digits. E.g., 2015)
* `month` (a number from 1 to 12. E.g., May should be 5)
* `count` (number of tweets mentioned the slogan in the month)
The result should be ordered by the count descendingly.

Hint: look at the documentation for parsing [strings](https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions) into dates and the [date functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions).

You should write your model on file q2.sql

## Question 3 (5 points)

Let's construct the Graph!

Twitter lets users mention another user in the tweet using the "@username" notation. (For the purposes of this project) the syntax for a mention is the character @ followed by one or more alphanumeric characters (capital letters A-Z, lowercase letters a-z, digits 0-9 and underline _). The username ends with either a non alphanumeric character or end of string.

For example, the string ".a@eugene123's" mentions eugene123. The string "@TIME a" mentions TIME.

Tweets and mentions can be used to construct the graph where each row is an edge from tweet_username to the username that is mentioned in the tweet text. For instance, if the username eugene tweeted "hi @elliot", then there would be an edge eugene -> elliot. We call eugene the src and elliot the dst. In this project, you can assume that there is at most one mention in a tweet, so you simply need to extract the first one.

Create a table “q3” with column names src and dst which stores the edge list of the graph. You must store only the distinct edges in the table. One user might mention another user in more than one tweet, but you should only save the edge once (i.e. only one row in the graph table). You should not store any edge whose src or dst is missing.

Note that creating a table simply means running 
```dbt run --select <path-to-q3.sql>``` in the dbt console.

You must save this table since you will be using it for the next few questions. Your table should contain the following columns:

  * `src`
  * `dst`
  
You should write your model on file q3.sql

You will need to refer to this table for the next questions using:
```
<dataset_name>.q3
```
for instance

```SQL
SELECT * FROM <dataset_name>.q3;
```

The dataset name is the one that you created on the setup phase and can be seen on your google cloud project (It's where your tables are stored!)


## Question 4 (3 points)
The indegree of a node in a directed graph is defined as the number of edges which are incoming on the node. Similarly, the outdegree of a node in a directed graph is defined as the number of edges which are outgoing from the node. For more information, you can read - Indegree and Outdegree

Using this information, find out from the GRAPH table which user has the highest indegree and which user has the highest outdegree.

Your answer should be a single query with a single tuple as the output. You need to run it and get the output containing the columns:

* `max_indegree` (contains the username that has been mentioned the most)
* `max_outdegree` (contains the username that has mentioned the most number of different users)


Note that, during grading, we will provide the right Graph table.

You should write your model on file q4.sql

## Question 5 (5 points)

Let us define 4 categories of Twitter users. For a given user U, we will use the number of users that mention U in their tweets (i.e. indegree) as the first metric, and the average number of likes that U receives for their tweets as the second metric. Then we can classify each user as follows:

High indegree, high average number of likes (popular users)  
High indegree, low average number of likes  
Low indegree, high average number of likes  
Low indegree, low average number of likes (unpopular users)  

We define the indegree and average number of likes to be high or low based on the rules below:

If indegree < avg(indegree of all users in the graph) then indegree is said to be low for the user, else it is considered high. A user is in the graph if it's either src or dst of an edge.

If avg(likes of all tweets for the user in the graph) < avg(likes for all tweets in tweets table), then the average number of likes is said to be low for the user, else it is considered high. A user without tweets has avg like 0.

Now, compute the following: given all tweets by unpopular users, what percentage of them mention a popular user? We will only consider users in the graph.

You can use temporary tables to do this question (no need to implement in a single query). Your final output should contain the column:

* `unpopular_popular`  


Cast your final result as FLOAT in unpopular_popular. For instance, if 50% of tweets by unpopular users mention a popular user, the final output should be 0.5.

You should write your model on file q5.sql

## Question 6 (6 points)

Given a graph G = (V, E), a “triangle” is a set of three different vertices that are mutually adjacent in G i.e. given 3 nodes of a graph A, B, C there exist edges A->B, B->C and C->A which form a triangle in the graph. From the graph table which you created above, find out the number of different triangles in the graph.

In a directed graph, the direction of the edges matter. A -> B -> C -> A and A -> C -> B -> A will count as 2 triangles.

Your answer should be a single query. You need to run it and get the output containing the column:

* `no_of_triangles`

You should write your model on file q6.sql

## Question 7, 8
See the [PageRank subfolder](https://github.com/w4111/project2-f22-template/tree/main/PageRank)
