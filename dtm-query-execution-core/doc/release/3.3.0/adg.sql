UPDATE "demo__december2020__transactions_actual" SET "amount" = 10 where "transaction_id" = 55555;
--
SELECT * FROM "demo__december2020__transactions_actual" where "transaction_id" = 1;
SELECT * FROM "demo__december2020__transactions_history" where "transaction_id" = 1;
