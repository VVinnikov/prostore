vshard = require'vshard'

load_lines('test_shares_accounts_actual',
    { {'C', 1, 1, 0, box.NULL}
    , {'D', 2, 1, 0, box.NULL}
    , {'D', 3, 1, 0, box.NULL}
})

load_lines('test_shares_transactions_actual',
    { {'2020-06-11', -100, 1, 1, 0, 0, box.NULL, vshard.router.bucket_id(1)}
    , {'2020-06-11', 100, 1, 2, 0, 0, box.NULL, vshard.router.bucket_id(2)}
})

load_lines('test_shares_transactions_actual',
    { {'2020-06-12', -50, 2, 2, 0, 1, box.NULL, vshard.router.bucket_id(2)}
    , {'2020-06-12', 50, 2, 3, 0, 1, box.NULL, vshard.router.bucket_id(3)}
})

load_lines('test_shares_transactions_actual',
    { {'2020-06-12', -20, 3, 3, 0, 1, box.NULL, vshard.router.bucket_id(3)}
    , {'2020-06-12', 20, 3, 1, 0, 1, box.NULL, vshard.router.bucket_id(1)}
})