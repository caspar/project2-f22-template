---- q1 -----
WITH ans AS (
    SELECT id, text
    FROM `graph.tweets`
    WHERE CONTAINS_SUBSTR(text, 'maga') AND CONTAINS_SUBSTR(text, 'trump')
)
select * from ans
---- q2 -----
WITH ans AS (
    SELECT extract(year from dt) as year, extract(month from dt) as month, count(*) as count
    FROM (SELECT PARSE_TIMESTAMP('%a %b %d %T %z %Y', create_time) as dt
            FROM `graph.tweets`
            WHERE CONTAINS_SUBSTR(text, 'maga'))
    GROUP BY month, year
    ORDER BY count(*) DESC
    LIMIT 5
)
select * from ans
---- q3 -----
WITH ans AS (
    SELECT DISTINCT src, SUBSTR(dst, 2) AS dst
    FROM (SELECT twitter_username AS src, REGEXP_EXTRACT(text, '@[a-zA-Z0-9_]+') AS dst
            FROM `graph.tweets`)
    WHERE STARTS_WITH(dst, '@')
)
select * from ans
---- q4 -----
WITH ans AS (
    SELECT (SELECT dst FROM {{ref('graph')}} GROUP BY dst ORDER BY count(*) desc LIMIT 1) as max_indegree,
            (SELECT src FROM {{ref('graph')}} GROUP BY src ORDER BY count(*) desc LIMIT 1) as max_outdegree
)
select * from ans
---- q5 -----
WITH indegree AS (
    (SELECT dst AS user, count(*) AS cnt
    FROM {{ref('graph')}} 
    GROUP BY dst)
    UNION DISTINCT
    (SELECT src AS user, 0 AS cnt
    FROM {{ref('graph')}}
    WHERE src not in (SELECT dst from {{ref('graph')}})
    GROUP BY src)
), avg_indegree AS (
    SELECT avg(cnt) AS cnt
    FROM indegree
), likes AS (
    SELECT twitter_username as user, avg(like_num) as likes
    FROM `graph.tweets`
    WHERE twitter_username in (SELECT dst FROM {{ref('graph')}}) or twitter_username in (SELECT src FROM {{ref('graph')}})
    GROUP BY twitter_username
), avg_likes AS (
    SELECT avg(like_num) AS likes
    FROM `graph.tweets`
), unpopular AS (
    SELECT indegree.user
    FROM indegree, likes
    WHERE indegree.user = likes.user AND indegree.cnt < (SELECT * FROM avg_indegree) AND likes.likes < (SELECT * FROM avg_likes)
), popular AS (
    SELECT indegree.user
    FROM indegree, likes
    WHERE indegree.user = likes.user AND indegree.cnt >= (SELECT * FROM avg_indegree) AND likes.likes >= (SELECT * FROM avg_likes)
), unpopular_tweets AS (
    SELECT twitter_username AS src, SUBSTR(REGEXP_EXTRACT(text, '@[a-zA-Z0-9_]+'), 2) AS dst 
    FROM `graph.tweets`
    WHERE twitter_username IN (SELECT * FROM unpopular)
), unpopular_tweets_popular AS (
    SELECT *
    FROM unpopular_tweets
    WHERE dst IN (SELECT * FROM popular)
), ans AS (
    SELECT CAST((SELECT COUNT(*) FROM unpopular_tweets_popular) AS FLOAT64)/(SELECT COUNT(*) FROM unpopular_tweets) AS unpopular_popular
)
select * from ans
---- q6 -----
WITH ans AS (
    SELECT COUNT(*) AS no_of_triangles
    FROM {{ref('graph')}} AS G1, {{ref('graph')}} AS G2, {{ref('graph')}} AS G3
    WHERE G1.dst = G2.src AND G1.dst = G2.src AND G2.dst = G3.src AND G3.dst = G1.src
    AND G1.src != G1.dst AND G2.src != G2.dst AND G3.src != G3.dst
    AND G1.src != G2.src AND G1.src != G3.src AND G2.src != G3.src 
)
select * from ans
---- q7 (pagerank1) -----
WITH nodes AS (
    (SELECT DISTINCT src AS usr
    FROM {{ref(‘graph’)}})
    UNION DISTINCT
    (SELECT DISTINCT dst AS usr
    FROM {{ref(‘graph’)}})
), outdegrees AS (
    SELECT src AS usr, COUNT(*) AS out
    FROM {{ref(‘graph’)}}
    GROUP BY src
), ranks AS (
    SELECT DISTINCT src as usr, 1/CAST((SELECT COUNT(*) FROM nodes) AS FLOAT64) AS Rank
    FROM {{ref(‘graph’)}}
), ans AS (
    SELECT g.dst AS usr, SUM(r.Rank/outdegrees.out) AS Rank
    FROM ranks AS r, {{ref(‘graph’)}} AS g, outdegrees
    WHERE r.usr = g.src AND r.usr = outdegrees.usr
    GROUP BY g.dst
    -- ORDER BY Rank DESC
    -- LIMIT 20
)
select * from ans
---- q8 (pagerank2) -----
with recursive outdegree as 
(
    select src as node, count(src) as outdeg
    from {{ref('q3')}}
    group by src
    union distinct
    select dst as node, 0 as outdeg
    from {{ref('q3')}}
    where dst not in (select src from {{ref('q3')}})

),
page_rank as (
    select node, 0 as iteration, 1/(select count(*) from outdegree) as partial_rank, outdeg
    from outdegree
    union ALL
    select graph.dst as node, pr.iteration +1 as iteration, pr.partial_rank/pr.outdeg as partial_rank, pr.outdeg as outdeg
    from {{ref('q3')}} graph, page_rank pr
    where pr.iteration < 2 and pr.node = graph.src
), 
ans as (
select node as username, sum(partial_rank) as page_rank_score
from page_rank
where iteration = 1
group by username
order by page_rank_score DESC
limit 20)
select * from ans