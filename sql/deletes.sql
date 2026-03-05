-- 1. Drop the public schema and EVERYTHING inside it (tables, views, etc.)
DROP SCHEMA public CASCADE;

-- 2. Recreate an empty public schema
CREATE SCHEMA public;

-- 3. Restore default permissions (optional but highly recommended)
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO public;