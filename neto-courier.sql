CREATE USER netocourier WITH PASSWORD 'NetoSQL2022';

GRANT ALL PRIVILEGES ON SCHEMA public TO netocourier;
   GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO netocourier;
   GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO netocourier;
   GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO netocourier;
  
  GRANT SELECT ON pg_catalog TO netocourier;
  
  GRANT USAGE ON SCHEMA information_schema TO netocourier;
   GRANT SELECT ON ALL TABLES IN SCHEMA information_schema TO netocourier;

  
  ALTER EXTENSION "uuid-ossp" SET SCHEMA public;
 
 
   GRANT USAGE ON SCHEMA pg_catalog TO netocourier;
   GRANT SELECT ON ALL TABLES IN SCHEMA pg_catalog TO netocourier;
  
  grant connect on database postgres 4 to netocourier

GRANT select all table ON SCHEMA pg_catalog TO postgres

GRANT USAGE ON SCHEMA information_schema TO netocourier;

grant select on table hr.person, hr.city to MyUser
  
  extensions.uuid_generate_v4()
  
  GRANT USAGE ON EXTENSION "uuid-ossp" TO netocourier;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

create table courier (
    id uuid primary key,
    from_place text,
    where_place text,
    name text,
    account_id uuid references account (id),
    contact_id uuid references contact (id),
    description text,
    user_id uuid references "user" (id),
    status public.status default 'В очереди',
    created_date timestamp with time zone default now()
  );

CREATE TABLE account (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  name VARCHAR
);

CREATE TABLE contact (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  last_name VARCHAR,
  first_name VARCHAR,
  account_id UUID REFERENCES account (id)
);

CREATE TABLE "user" (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  last_name VARCHAR,
  first_name VARCHAR,
  dismissed BOOLEAN DEFAULT false
);

CREATE TYPE public.status AS ENUM ('В очереди', 'Выполняется', 'Выполнено', 'Отменен');






DROP TYPE public."_status";

DROP TYPE status;



create or replace procedure add_courier (
  from_place text,
  where_place text,
  name text,
  account_id uuid,
  contact_id uuid,
  description text,
  user_id uuid
) as $$
BEGIN
INSERT INTO courier (from_place, where_place, name, account_id, contact_id, description, user_id)
VALUES (from_place, where_place, name, account_id, contact_id, description, user_id);
END;
$$ language plpgsql;



create or replace function get_courier () returns table (
  id uuid,
  from_place text,
  where_place text,
  name text,
  account_id uuid,
  account text,
  contact_id uuid,
  contact text,
  description text,
  user_id uuid,
  "user" text,
  status public.status,
  created_date timestamp
) as $$
BEGIN
SELECT
  c.id,
  c.from_place,
  c.where_place,
  c.name,
  a.id AS account_id,
  a.name AS account,
  ct.id AS contact_id,
  CONCAT(ct.last_name, ' ', ct.first_name) AS contact,
  c.description,
  u.id AS user_id,
  CONCAT(u.last_name, ' ', u.first_name) AS user,
  c.status,
  c.created_date
FROM courier c
LEFT JOIN account a ON c.account_id = a.id
LEFT JOIN contact ct ON c.contact_id = ct.id
LEFT JOIN user u ON c.user_id = u.id
ORDER BY status DESC, created_date DESC;
END;
$$ language plpgsql;


CREATE OR REPLACE PROCEDURE change_status(STATUS, UUID)
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE courier
    SET status = $1
    WHERE id = $2;
END;
$$;

CALL change_status('Выполнено', 'c3205825-ab75-4352-af0d-3dd945aed468')

CREATE OR REPLACE FUNCTION get_users()
RETURNS TABLE (
  "user" text
) AS $$
BEGIN
SELECT CONCAT(u.last_name, ' ', u.first_name) AS user
FROM "user" u
WHERE dismissed = false
ORDER BY u.last_name;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_accounts()
RETURNS TABLE (
  account text
) AS $$
BEGIN
SELECT name AS account
FROM account
ORDER BY name;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_contacts(account_id UUID)
RETURNS TABLE (
  contact text
) AS $$
BEGIN
IF account_id IS NULL THEN
    RAISE EXCEPTION 'Выберите контрагента';
END IF;
SELECT CONCAT(c.last_name, ' ', c.first_name) AS contact
FROM contact c
WHERE c.account_id = account_id
ORDER BY c.last_name;
END;
$$ LANGUAGE plpgsql;


CREATE VIEW courier_statistic AS
SELECT account.id AS account_id,
       account.name AS account,
       COUNT(courier.account_id) AS count_courier,
       SUM(CASE WHEN courier.status = 'Выполнено' THEN 1 ELSE 0 END) AS count_complete,
       SUM(CASE WHEN courier.status = 'Отменен' THEN 1 ELSE 0 END) AS count_canceled,
       (COUNT(courier.account_id) / prev_month.count_courier) * 100 AS percent_relative_prev_month,
       COUNT(DISTINCT courier.where_place) AS count_where_place,
       COUNT(contact.id) AS count_contact,
       ARRAY_AGG("user".id) AS cancel_user_array
FROM courier
LEFT JOIN account ON courier.account_id = account.id
LEFT JOIN (
    SELECT account_id, COUNT(account_id) AS count_courier
    FROM courier
    GROUP BY account_id
) prev_month ON account.id = prev_month.account_id
LEFT JOIN contact ON courier.contact_id = contact.id
LEFT JOIN "user" ON courier.user_id = "user".id
GROUP BY account.id, account.name, prev_month.count_courier;


CREATE VIEW courier_statistic as 
SELECT
    a.id AS account_id,
    a.name AS account,
    COUNT(DISTINCT c.id) AS count_courier,
    SUM(CASE WHEN c.status = 'Выполнено' THEN 1 ELSE 0 END) AS count_complete,
    SUM(CASE WHEN c.status = 'Отменен' THEN 1 ELSE 0 END) AS count_canceled,
     coalesce(
    (
      count(*) filter (
        where
          date_trunc('month', c.created_date) = date_trunc('month', current_date)
          and c.account_id = c.account_id
      ) - count(*) filter (
        where
          date_trunc('month', c.created_date) = date_trunc('month', current_date) - interval '1 month'
          and c.account_id = c.account_id
      )
    ) / nullif(
      count(*) filter (
        where
          date_trunc('month', c.created_date) = date_trunc('month', current_date) - interval '1 month'
          and c.account_id = c.account_id
      ),
      0
    ) * 100,
    0
  ) as percent_relative_prev_month,
    COUNT(DISTINCT c.where_place) AS count_where_place,
    COUNT(CASE WHEN c.status = 'Выполняется' THEN ct.id ELSE NULL END) AS count_contact,
    ARRAY_AGG(DISTINCT u.id) FILTER (WHERE c.user_id = u.id AND c.status = 'Отменен') AS cansel_user_array
FROM courier c
JOIN account a ON c.account_id = a.id
JOIN contact ct ON c.contact_id = ct.id
JOIN "user" u ON c.user_id = u.id
GROUP BY c.account_id, a.id, a.name;



SELECT 
    account_id,
    COALESCE(((SELECT COUNT(*)  FROM courier   WHERE account_id = c.account_id    AND DATE_TRUNC('month', created_date) = DATE_TRUNC('month', CURRENT_DATE))  - (SELECT COUNT(*) 
          FROM courier 
          WHERE account_id = c.account_id 
          AND DATE_TRUNC('month', created_date) = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month')
        ) / NULLIF(
            (SELECT COUNT(*) 
             FROM courier 
             WHERE account_id = c.account_id 
             AND DATE_TRUNC('month', created_date) = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'),
            0
        ) * 100, 
        0
    ) AS percentage_change
FROM 
    courier c 
GROUP BY 
    account_id;

select COUNT(DISTINCT c.id) AS count_contact
from courier c 
group by account_id 

select --COUNT(DISTINCT c.id)- 
SUM(CASE WHEN c.status = 'Выполнено' THEN 1 ELSE 0 END)-- /
--COUNT(DISTINCT c.id) * 100
from courier c 

select SUM(CASE WHEN c.status = 'Выполнено' THEN 1 ELSE 0 END) AS count_complete
from courier c 
    
    
create
or replace procedure insert_test_data (value integer) language plpgsql as $$
BEGIN
    -- Вносим value * 1 строк случайных данных в отношение account.
    INSERT INTO account (id, name)
    SELECT uuid_generate_v4(),
           (select substr(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя',1,((random() * 33)+1)::integer), ((random() * 5)+1)::integer),1,20))
    FROM generate_series(1, value);

   -- Вносим value * 2 строк случайных данных в отношение contact.
    INSERT INTO contact (id, last_name, first_name, account_id)
    SELECT uuid_generate_v4(),
           (select substr(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя',1,((random() * 33)+1)::integer), ((random() * 5)+1)::integer),1,50)),
		   (select substr(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя',1,((random() * 33)+1)::integer), ((random() * 5)+1)::integer),1,50)),
		   (SELECT id FROM account ORDER BY random() limit 1)
	FROM generate_series(1, value * 2);

    -- Вносим value * 1 строк случайных данных в отношение user.
    INSERT INTO "user" (id, last_name, first_name, dismissed)
    SELECT uuid_generate_v4(),
           (select substr(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя',1,((random() * 33)+1)::integer), ((random() * 5)+1)::integer),1,50)),
		   (select substr(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя',1,((random() * 33)+1)::integer), ((random() * 5)+1)::integer),1,50)),
		   RANDOM() < 0.5
	FROM generate_series(1, value);

    -- Вносим value * 5 строк случайных данных в отношение courier.
    INSERT INTO courier (id, from_place, where_place, name, account_id, contact_id, description, user_id, status, created_date)
    SELECT uuid_generate_v4(),
           (SELECT substr(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя',1,((random() * 33)+1)::integer), ((random() * 5)+1)::integer),1,150)),
           (select substr(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя',1,((random() * 33)+1)::integer), ((random() * 5)+1)::integer),1,150)),
           (select substr(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя',1,((random() * 33)+1)::integer), ((random() * 5)+1)::integer),1,20)),
           (SELECT id FROM account ORDER BY random() LIMIT 1),
           (SELECT id FROM contact ORDER BY random() LIMIT 1),
           (select repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя', 1, (random() * 33)::integer), (random() * 10)::integer)),
           (SELECT id FROM "user" ORDER BY random() LIMIT 1),
           (select* from  unnest(enum_range(NULL::status)) ORDER BY random() limit 1),
           (select now() - interval '1 day' * round(random() * 1000) as timestamp)
    FROM generate_series(1, value * 5);
END;
$$;


CREATE OR REPLACE PROCEDURE insert_test_data (value INTEGER)
LANGUAGE plpgsql AS
$BODY$
DECLARE
    acc_count INT;
    cont_count INT;
    usr_count INT;
    cur_courier_id UUID;
BEGIN
    -- Вставляем value * 1 строк в account
    FOR acc_count IN 1..value LOOP
        INSERT INTO account (id, name)
        VALUES (extensions.uuid_generate_v4(),
                (SELECT substr(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя',1,((random() * 33)+1)::integer), ((random() * 5)+1)::integer),1,20)));
    END LOOP;

    -- Вставляем value * 2 строк в contact
    FOR cont_count IN 1..(2 * value) LOOP
        INSERT INTO contact (id, last_name, first_name, account_id)
        VALUES (extensions.uuid_generate_v4(),
                (SELECT substr(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя',1,((random() * 33)+1)::integer), ((random() * 5)+1)::integer),1,50)),
               (SELECT substr(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя',1,((random() * 33)+1)::integer), ((random() * 5)+1)::integer),1,50)),
               (SELECT id FROM account ORDER BY random() LIMIT 1));
    END LOOP;

    -- Вставляем value * 1 строк в user
    FOR usr_count IN 1..value LOOP
        INSERT INTO "user" (id, last_name, first_name, dismissed)
        VALUES (extensions.uuid_generate_v4(),
                (SELECT substr(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя',1,((random() * 33)+1)::integer), ((random() * 5)+1)::integer),1,50)),
               (SELECT substr(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя',1,((random() * 33)+1)::integer), ((random() * 5)+1)::integer),1,50)),
               random() < 0.5);
    END LOOP;

    -- Вставляем value * 5 строк в courier
    FOR cur_courier_id IN 1..(5 * value) LOOP
        INSERT INTO courier (id, from_place, where_place, name, account_id, contact_id, description, user_id, status, created_date)
        VALUES (extensions.uuid_generate_v4(),
               (SELECT substr(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя',1,((random() * 33)+1)::integer), ((random() * 5)+1)::integer),1,150)),
               (SELECT substr(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя',1,((random() * 33)+1)::integer), ((random() * 5)+1)::integer),1,150)),
               (SELECT substr(repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя',1,((random() * 33)+1)::integer), ((random() * 5)+1)::integer),1,20)),
               (SELECT id FROM account ORDER BY random() LIMIT 1),
               (SELECT id FROM contact ORDER BY random() LIMIT 1),
               (SELECT repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя', 1, (random() * 33)::integer), (random() * 10)::integer)),
           	   (SELECT id FROM "user" ORDER BY random() LIMIT 1),
               (select* from  unnest(enum_range(NULL::status)) ORDER BY random() limit 1),
               (select now() - interval '1 day' * round(random() * 1000) as timestamp));
    END LOOP;
END;
$BODY$;


CREATE OR REPLACE PROCEDURE erase_test_data()
LANGUAGE plpgsql
AS $$
BEGIN

	 -- Удаляем данные из таблицы courier
    DELETE FROM courier;

    -- Удаляем данные из таблицы contact
    DELETE FROM contact;

    -- Удаляем данные из таблицы account
    DELETE FROM account;

 	DELETE FROM "user";

    RAISE NOTICE 'Данные успешно удалены из всех тестовых отношений.';
END;
$$;

CALL insert_test_data(10);

CALL erase_test_data();

SELECT * FROM unnest(enum_range(NULL::status)) ORDER BY random() LIMIT 1;


select * from courier_statistic cs 

select * from pg_catalog

SELECT * FROM pg_available_extensions WHERE name LIKE 'uuid-ossp';


CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


SELECT extensions.uuid_generate_v4()


GRANT EXECUTE ON FUNCTION uuid_generate_v4() TO netocourier;

select * from get_contacts('62ea9fe6-8215-462e-afba-8ae2cf60de8a')


select * from get_contacts(null)

select * from courier
order by created_date desc

select * from "user"

select * from courier

SELECT * FROM courier_statistic

SELECT * FROM get_accounts()


SELECT * FROM get_users()


CALL change_status('Отменен', 'c3205825-ab75-4352-af0d-3dd945aed468')


CALL add_courier ('ffdfdsfdf', 'fwfwfwff', 'fwfwfwff', '62ea9fe6-8215-462e-afba-8ae2cf60de8a', '16bbfa5a-0a19-4dca-a286-b2b9c7000d07', 'dsdsdsad', '0b055586-17bf-47a6-bf63-aa78164cbe17')


