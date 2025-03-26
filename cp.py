ifelse(
    ${MetricToDisplay} = 'Unique Users',
    ifelse(
        distinct_count(ifelse({Organization_CP} = 'CrossPlay', {nr_sbsc_hash_pii}, NULL)) < 15, 
        NULL, 
        distinct_count(ifelse({Organization_CP} = 'CrossPlay', {nr_sbsc_hash_pii}, NULL))
    ),
    
    ${MetricToDisplay} = 'Tx Count',
    ifelse(
        ${crossPlay} = 'Yes',
        sum(ifelse({Organization_CP} = 'CrossPlay', {tx_count}, NULL)),
        sum({tx_count})  -- Show total Tx count if "No" is selected
    ),
    
    ${MetricToDisplay} = 'Tx Value',
    ifelse(
        ${crossPlay} = 'Yes',
        sum(ifelse({Organization_CP} = 'CrossPlay', {tx_value}, NULL)),
        sum({tx_value})  -- Show total value if "No" is selected
    ),
    
    NULL -- Default case if none of the conditions match
)
