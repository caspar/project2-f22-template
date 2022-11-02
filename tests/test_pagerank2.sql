select page_rank_score
from {{ref('pagerank2.sql')}}
where page_rank_score = 0 