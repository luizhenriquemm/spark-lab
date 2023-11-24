CREATE TABLE IF NOT EXISTS clients (
    client_id SERIAL NOT NULL,
    name VARCHAR(64) NOT NULL,
    gender VARCHAR(16) NOT NULL,
    birthdate VARCHAR(16) NOT NULL,
    address VARCHAR(64) NOT NULL,
    city VARCHAR(64) NOT NULL,
    state VARCHAR(64) NOT NULL,
    PRIMARY KEY(client_id)
);

CREATE TABLE IF NOT EXISTS categories (
    category_id SERIAL NOT NULL,
    name VARCHAR(64) NOT NULL,
    PRIMARY KEY(category_id)
);

CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL NOT NULL,
    name VARCHAR(64) NOT NULL,
    value NUMERIC NOT NULL,
    category_id INT NOT NULL,
    PRIMARY KEY(product_id),
    CONSTRAINT fk_category
        FOREIGN KEY(category_id)
	        REFERENCES categories(category_id)
);

CREATE TABLE IF NOT EXISTS promotions (
    promotion_id SERIAL NOT NULL,
    name VARCHAR(64) NOT NULL,
    discount NUMERIC NOT NULL,
    product_id INT NOT NULL,
    PRIMARY KEY(promotion_id),
    CONSTRAINT fk_product
        FOREIGN KEY(product_id)
	        REFERENCES products(product_id)
);

CREATE TABLE IF NOT EXISTS sales (
    sale_id SERIAL NOT NULL,
    client_id INT NOT NULL,
    total_value NUMERIC NOT NULL,
    sale_timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    PRIMARY KEY(sale_id),
    CONSTRAINT fk_client
        FOREIGN KEY(client_id)
	        REFERENCES clients(client_id)
);

CREATE TABLE IF NOT EXISTS sale_items (
    item_id SERIAL NOT NULL,
    product_id INT NOT NULL,
    sale_id INT NOT NULL,
    quantity INT NOT NULL,
    unitary_value NUMERIC NOT NULL,

    PRIMARY KEY(item_id),
    CONSTRAINT fk_product
        FOREIGN KEY(product_id)
	        REFERENCES products(product_id),
    CONSTRAINT fk_sale
        FOREIGN KEY(sale_id)
	        REFERENCES sales(sale_id)
);