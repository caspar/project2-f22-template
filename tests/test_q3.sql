select count(*) as ct
from {{ref('q3')}}
having ct < 10