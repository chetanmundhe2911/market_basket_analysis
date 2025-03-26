ifelse(
    ${MetricToDisplay} = 'Unique Users',
    ifelse(
        ${crossPlay} = 'Yes', 
        ifelse(distinct_count(ifelse({Organization_CP} = 'CrossPlay', {nr_sbsc_hash_pii}, NULL)) < 15, 
               NULL, 
               distinct_count(ifelse({Organization_CP} = 'CrossPlay', {nr_sbsc_hash_pii}, NULL))
        ),
        distinct_count({nr_sbsc_hash_pii})
    ),

    ${MetricToDisplay} = 'Tx Count',
    ifelse(
        ${crossPlay} = 'Yes',
        sum(ifelse({Organization_CP} = 'CrossPlay', {tx_count}, NULL)),
        sum({tx_count})  -- Counts all Tx if No is selected
    ),

    sum(ifelse({Organization_CP} = 'CrossPlay', {tx_value}, NULL))
)
