# Практическая работа 5

## Описание

В этой практической работе вы будете настраивать Debezium Connector для передачи данных из базы данных PostgreSQL в
Apache Kafka с использованием механизма Change Data Capture (CDC).

Цель практической работы — закрепить знания о работе с коннекторами, а также научиться собирать и анализировать метрики.

Практическая работа состоит из двух обязательных заданий.

## Как выполнить практическую работу

Выполняйте задания на своём компьютере, использовать облачные сервисы необязательно. Храните материалы в вашем
репозитории на GitHub.

### Задание 1. Настройка Debezium Connector для PostgreSQL

1. Создайте файл `docker-compose.yaml`. В нём должны присутствовать следующие компоненты:

    - Apache Kafka
    - Kafka Connect
    - PostgreSQL

2. Создайте базу данных PostgreSQL и таблицы, которые будут использоваться для работы — `users` и `orders` со следующими
   структурами:

   ```sql
   CREATE TABLE users (
       id SERIAL PRIMARY KEY,
       name VARCHAR(100),
       email VARCHAR(100),
       created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
   );

   CREATE TABLE orders (
       id SERIAL PRIMARY KEY,
       user_id INT REFERENCES users(id),
       product_name VARCHAR(100),
       quantity INT,
       order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
   );
   ```

3. Настройте Debezium Connector для отслеживания изменений ТОЛЬКО в таблицах `users` и `orders`.
4. Проверьте статус коннектора и убедитесь, что он работает корректно.
5. Получите данные из топиков Apache Kafka, используя код на выбранном вами языке программирования, и выведите их в
   терминал.

### Задание 2. Мониторинг и анализ метрик

1. Настройте Prometheus для сбора метрик из Kafka Connect.
2. Создайте графики в Grafana для визуализации метрик передачи данных и мониторинга работоспособности Kafka Connector.

### Как должен выглядеть результат

Выполненное задание должно включать компоненты:

- Брокер Kafka, настроенный для обработки данных.
- База данных PostgreSQL с таблицами `users` и `orders`.
- Kafka Connect, настроенный для работы с Debezium Connector.
- Файл с настройками для конфигурации Debezium Connector.

В файле `README.md` подробно опишите:

- Инструкции по запуску решения через Docker Compose.
- Назначение каждого компонента и их взаимосвязи.
- Настройки Debezium Connector.
- Пошаговые инструкции по проверке работоспособности решения.

### Пример данных для тестирования

Добавьте несколько записей в таблицы `users` и `orders` для тестирования.

```sql
-- Добавление пользователей
INSERT INTO users (name, email)
VALUES ('John Doe', 'john@example.com');
INSERT INTO users (name, email)
VALUES ('Jane Smith', 'jane@example.com');
INSERT INTO users (name, email)
VALUES ('Alice Johnson', 'alice@example.com');
INSERT INTO users (name, email)
VALUES ('Bob Brown', 'bob@example.com');

-- Добавление заказов
INSERT INTO orders (user_id, product_name, quantity)
VALUES (1, 'Product A', 2);
INSERT INTO orders (user_id, product_name, quantity)
VALUES (1, 'Product B', 1);
INSERT INTO orders (user_id, product_name, quantity)
VALUES (2, 'Product C', 5);
INSERT INTO orders (user_id, product_name, quantity)
VALUES (3, 'Product D', 3);
INSERT INTO orders (user_id, product_name, quantity)
VALUES (4, 'Product E', 4);
```

## Как отправить работу на проверку

1. Выполните практическую работу. Решение залейте в папку вашего репозитория на GitHub.
2. Сделайте репозиторий доступным для ревью.
3. Пришлите ссылку на GitHub.
