-- CH-benCHmark Schema DDL
-- ===========================
-- Spec 03 / Spec 04: Cross-table atomicity + FK constraints defined via RawWise SQL dialect.
-- All tables use composite primary keys matching TPC-C/CH-benCHmark spec.

CREATE TABLE IF NOT EXISTS db.warehouse (
    w_id     INT NOT NULL,
    w_name   VARCHAR(10),
    w_street_1 VARCHAR(20),
    w_street_2 VARCHAR(20),
    w_city   VARCHAR(20),
    w_state  CHAR(2),
    w_zip    CHAR(9),
    w_tax    DECIMAL(4,4),
    w_ytd    DECIMAL(12,2),
    PRIMARY KEY (w_id)
);

CREATE TABLE IF NOT EXISTS db.district (
    d_w_id    INT NOT NULL,
    d_id      INT NOT NULL,
    d_name    VARCHAR(10),
    d_street_1 VARCHAR(20),
    d_street_2 VARCHAR(20),
    d_city    VARCHAR(20),
    d_state   CHAR(2),
    d_zip     CHAR(9),
    d_tax     DECIMAL(4,4),
    d_ytd     DECIMAL(12,2),
    d_next_o_id INT,
    PRIMARY KEY (d_w_id, d_id)
);

CREATE TABLE IF NOT EXISTS db.customer (
    c_w_id         INT NOT NULL,
    c_d_id         INT NOT NULL,
    c_id           INT NOT NULL,
    c_first        VARCHAR(16),
    c_middle       CHAR(2),
    c_last         VARCHAR(16),
    c_street_1     VARCHAR(20),
    c_street_2     VARCHAR(20),
    c_city         VARCHAR(20),
    c_state        CHAR(2),
    c_zip          CHAR(9),
    c_phone        CHAR(16),
    c_since        TIMESTAMP,
    c_credit       CHAR(2),
    c_credit_lim   DECIMAL(12,2),
    c_discount     DECIMAL(4,4),
    c_balance      DECIMAL(12,2),
    c_ytd_payment  DECIMAL(12,2),
    c_payment_cnt  INT,
    c_delivery_cnt INT,
    c_data         VARCHAR(500),
    PRIMARY KEY (c_w_id, c_d_id, c_id)
);

CREATE TABLE IF NOT EXISTS db.orders (
    o_w_id       INT NOT NULL,
    o_d_id       INT NOT NULL,
    o_id         INT NOT NULL,
    o_c_id       INT,
    o_carrier_id INT,
    o_ol_cnt     INT,
    o_all_local  INT,
    o_entry_d    TIMESTAMP,
    PRIMARY KEY (o_w_id, o_d_id, o_id)
);

CREATE TABLE IF NOT EXISTS db.order_line (
    ol_w_id        INT NOT NULL,
    ol_d_id        INT NOT NULL,
    ol_o_id        INT NOT NULL,
    ol_number      INT NOT NULL,
    ol_i_id        INT,
    ol_supply_w_id INT,
    ol_quantity    INT,
    ol_amount      DECIMAL(6,2),
    ol_delivery_d  TIMESTAMP,
    ol_dist_info   CHAR(24),
    PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
);

CREATE TABLE IF NOT EXISTS db.item (
    i_id    INT NOT NULL,
    i_name  VARCHAR(24),
    i_price DECIMAL(5,2),
    i_data  VARCHAR(50),
    i_im_id INT,
    PRIMARY KEY (i_id)
);

CREATE TABLE IF NOT EXISTS db.stock (
    s_w_id   INT NOT NULL,
    s_i_id   INT NOT NULL,
    s_quantity INT,
    s_ytd    INT,
    s_order_cnt INT,
    s_remote_cnt INT,
    s_data   VARCHAR(50),
    s_dist_01 CHAR(24),
    s_dist_02 CHAR(24),
    s_dist_03 CHAR(24),
    s_dist_04 CHAR(24),
    s_dist_05 CHAR(24),
    s_dist_06 CHAR(24),
    s_dist_07 CHAR(24),
    s_dist_08 CHAR(24),
    s_dist_09 CHAR(24),
    s_dist_10 CHAR(24),
    PRIMARY KEY (s_w_id, s_i_id)
);

-- Spec 04: Secondary index on orders for Delivery transaction lookup
CREATE INDEX idx_orders_carrier ON db.orders (o_carrier_id);

-- Spec 04: Secondary index on customer (c_last) for Payment lookup
CREATE INDEX idx_customer_last ON db.customer (c_w_id, c_d_id, c_last);
