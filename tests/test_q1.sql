select count(*) as ct
from {{ref('q1')}}
having ct < 1