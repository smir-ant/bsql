-- Add a display name and make the bio required.
ALTER TABLE users ADD COLUMN name TEXT NOT NULL;
ALTER TABLE users ALTER COLUMN bio SET NOT NULL;
