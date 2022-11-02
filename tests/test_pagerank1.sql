select page_rank_score
from {{ref('pagerank1.sql')}}
where page_rank_score = 0 