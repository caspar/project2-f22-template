# Extra Credit Question 7 (4 points)
The PageRank algorithm is used to rank the importance of nodes in a graph. It works by counting the number of edges incident to a node to determine how important the node is. The underlying assumption is that more important nodes are likely to receive more links from other nodes. Find the top 20 nodes with the highest PageRank in the graph.

You must run the algorithm for **1 iteration of pagerank** and your output table should contain the following columns:

* `username` (the twitter_username of the user)
* `page_rank_score`

This algorithm works as follows - Assume a small universe of four web pages: A, B, C and D. PageRank is initialized to the same value for all pages since we assume a probability distribution between 0 and 1 as the PageRank for each node. Hence the initial value for each page in this example is 0.25. If the only links in the system were from pages B->A, C->A and D->A, each link would transfer 0.25 PageRank to A upon the next iteration, for a total of 0.75 i.e.

$$PR_{1}(A) = PR(B) + PR(C) + PR(D)$$

Now, suppose instead that we have the links B->C, B->A, C->A, D->A, D->B, D->C. Thus, upon the first iteration, page B would transfer half of its existing value, or 0.125, to page A and the other half, or 0.125, to page C. Page C would transfer all of its existing value, 0.25, to the only page it links to, A. Since D had three outbound links, it would transfer one third of its existing value, or approximately 0.083, to A. At the completion of this iteration, page A will have a PageRank of approximately 0.458. PR(A)=PR(B)/2 + PR(C)/1 + PR(D)/3.

Thus, we can write the PageRank of A as: $PR(A)= \frac{PR(B)}{L(B)} + \frac{PR(C)}{L(C)} + PR(D)/L(D)$ where L(x) gives us the number of outbound links for any node x, and

In general, the PageRank value for a page u is dependent on the PageRank values for each page v contained in the set containing all pages linking to page u, divided by the number of links from page v. It is given by the formula: 

$$PR(u) = \sum_{v \in B_u}^{\frac{PR(v)}{L(v)}}$$

A node u without inbound nodes will have empty $B_u$, and a node u without outbound nodes won't contribute to the PageRank of any other nodes.

To read more about PageRank, you can refer to the following link: [PageRank](http://ilpubs.stanford.edu:8090/422/1/1999-66.pdf)

## Write your solution on Pagerank1.sql

You can store your intermediate result in any temp tables.

Make sure you return an output with the following columns:

* `username` (the twitter_username of the user)
* `page_rank_score` (type should be double)

You **Don't need to execute multiple iterations of pagerank.** Just run the first iteration

Please don't include twitter_username that is not in the Graph table as graph nodes. Only the Graph table is needed for this question. Don't include users with page_rank_score 0.

# Extra Credit Question 8 (4 points)
Answer the page rank question using a single recursive query.

To get started, read the SQL slides about recursive WITH clauses. In the Fibonacci example, each iteration of the `WITH RECURSIVE fib` clause (recursive step) simply queried fib to produce new tuples.

# Check Your Results

Make sure all the following queries are available. We will grade your project based on these queries.

Make sure the query outputs has the same schema as specified (check the name and order of columns, name is case sensitive!)

There shouldn't be any query modifying tweets table and only q3 creates and modifies Graph table. If you have additional queries that modify these two tables, the modification may affect the grading of all queries and we don't accept regrade request because of that.

You will be submitting .sql files, 
