ifelse(
    ${MetricToDisplay} = 'Unique Users',
    ifelse(
        distinct_count(
            ifelse(
                ${crossPlay} = 'Yes' AND {Organization_CP} = 'CrossPlay',
                {nr_sbsc_hash_pii},
                ifelse(${crossPlay} = 'No', {nr_sbsc_hash_pii}, NULL)
            )
        ) < 15,
        NULL,
        distinct_count(
            ifelse(
                ${crossPlay} = 'Yes' AND {Organization_CP} = 'CrossPlay',
                {nr_sbsc_hash_pii},
                ifelse(${crossPlay} = 'No', {nr_sbsc_hash_pii}, NULL)
            )
        )
    ),

    ${MetricToDisplay} = 'Tx Count',
    ifelse(
        ${crossPlay} = 'Yes',
        sum(ifelse({Organization_CP} = 'CrossPlay', {tx_count}, NULL)),
        sum({tx_count}) -- If "No" is selected, sum all transactions
    ),

    ${MetricToDisplay} = 'Tx Value',
    ifelse(
        ${crossPlay} = 'Yes',
        sum(ifelse({Organization_CP} = 'CrossPlay', {tx_value}, NULL)),
        sum({tx_value}) -- If "No" is selected, sum all transaction values
    ),

    ${MetricToDisplay} = 'All',
    ifelse(
        ${crossPlay} = 'Yes',
        sum(ifelse({Organization_CP} = 'CrossPlay', {tx_count}, NULL)) +
        sum(ifelse({Organization_CP} = 'CrossPlay', {tx_value}, NULL)) +
        distinct_count(ifelse({Organization_CP} = 'CrossPlay', {nr_sbsc_hash_pii}, NULL)),
        
        sum({tx_count}) +
        sum({tx_value}) +
        distinct_count({nr_sbsc_hash_pii}) -- If "No" is selected, use all data
    ),

    NULL -- Default return value if nothing matches
)


========================
