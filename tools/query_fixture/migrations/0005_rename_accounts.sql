-- Rename the table. After this, `accounts.*` resolves and
-- `legacy_accounts.*` does not (the old-name reference fails to compile).
ALTER TABLE legacy_accounts RENAME TO accounts;
