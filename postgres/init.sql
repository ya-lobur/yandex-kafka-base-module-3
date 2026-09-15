CREATE TABLE public.users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE public.orders (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    product_name VARCHAR(100),
    quantity INT,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Полный before делает UPDATE/DELETE наглядными; ценой дополнительного WAL.
ALTER TABLE public.users REPLICA IDENTITY FULL;
ALTER TABLE public.orders REPLICA IDENTITY FULL;

INSERT INTO public.users (name, email) VALUES
    ('John Doe', 'john@example.com'), ('Jane Smith', 'jane@example.com'),
    ('Alice Johnson', 'alice@example.com'), ('Bob Brown', 'bob@example.com');
INSERT INTO public.orders (user_id, product_name, quantity) VALUES
    (1, 'Product A', 2), (1, 'Product B', 1), (2, 'Product C', 5),
    (3, 'Product D', 3), (4, 'Product E', 4);

-- Только учебные реквизиты. Publication создаёт администратор, не Debezium.
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium-pw';
GRANT CONNECT ON DATABASE shop TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON public.users, public.orders TO debezium;
CREATE PUBLICATION practice5_publication FOR TABLE public.users, public.orders;
