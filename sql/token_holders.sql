select op.address
    ,sum(ast.value) as amount
from node_outputs op
    join node_assets ast on ast.box_id = op.box_id
        and ast.header_id = op.header_id
        and ast.token_id = '%s'
    left join node_inputs ip on ip.box_id = op.box_id 
where ip.box_id is null
    and op.main_chain = true
group by op.address
order by sum(ast.value) desc
