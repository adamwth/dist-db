CREATE TABLE IF NOT EXISTS "warehouse" (
  "w_id" int PRIMARY KEY,
  "w_name" varchar(10),
  "w_street_1" varchar(20),
  "w_street_2" varchar(20),
  "w_city" varchar(20),
  "w_state" char(2),
  "w_zip" char(9),
  "w_tax" decimal(4,4),
  "w_ytd" decimal(12,2)
);

CREATE TABLE IF NOT EXISTS "district" (
  "d_w_id" int,
  "d_id" int,
  "d_name" varchar(10),
  "d_street_1" varchar(20),
  "d_street_2" varchar(20),
  "d_city" varchar(20),
  "d_state" char(2),
  "d_zip" char(9),
  "d_tax" decimal(4,4),
  "d_ytd" decimal(12,2),
  "d_next_o_id" int,
  PRIMARY KEY ("d_w_id", "d_id")
);

CREATE TABLE IF NOT EXISTS "customer" (
  "c_w_id" int,
  "c_d_id" int,
  "c_id" int,
  "c_first" varchar(16),
  "c_middle" char(2),
  "c_last" varchar(16),
  "c_street_1" varchar(20),
  "c_street_2" varchar(20),
  "c_city" varchar(20),
  "c_state" char(2),
  "c_zip" char(9),
  "c_phone" char(16),
  "c_since" timestamp,
  "c_credit" char(2),
  "c_credit_lim" decimal(12,2),
  "c_discount" decimal(4,4),
  "c_balance" decimal(12,2),
  "c_ytd_payment" float,
  "c_payment_cnt" int,
  "c_delivery_cnt" int,
  "c_data" varchar(500),
  PRIMARY KEY ("c_w_id", "c_d_id", "c_id")
);

CREATE TABLE IF NOT EXISTS "order" (
  "o_w_id" int,
  "o_d_id" int,
  "o_id" int,
  "o_c_id" int,
  "o_carrier_id" int,
  "o_ol_cnt" decimal(2,0),
  "o_all_local" decimal(1,0),
  "o_entry_d" timestamp,
  PRIMARY KEY ("o_w_id", "o_d_id", "o_id")
);

CREATE TABLE IF NOT EXISTS "item" (
  "i_id" int PRIMARY KEY,
  "i_name" varchar(24),
  "i_price" decimal(5,2),
  "i_im_id" int,
  "i_data" varchar(50)
);

CREATE TABLE IF NOT EXISTS "orderline" (
  "ol_w_id" int,
  "ol_d_id" int,
  "ol_o_id" int,
  "ol_number" int,
  "ol_i_id" int,
  "ol_delivery_d" timestamp,
  "ol_amount" decimal(6,2),
  "ol_supply_w_id" int,
  "ol_quantity" decimal(2,0),
  "ol_dist_info" char(24),
  PRIMARY KEY ("ol_w_id", "ol_d_id", "ol_o_id", "ol_number")
);

CREATE TABLE IF NOT EXISTS "stock" (
  "s_w_id" int,
  "s_i_id" int,
  "s_quantity" decimal(4,0),
  "s_ytd" decimal(8,2),
  "s_order_cnt" int,
  "s_remote_cnt" int,
  "s_dist_01" char(24),
  "s_dist_02" char(24),
  "s_dist_03" char(24),
  "s_dist_04" char(24),
  "s_dist_05" char(24),
  "s_dist_06" char(24),
  "s_dist_07" char(24),
  "s_dist_08" char(24),
  "s_dist_09" char(24),
  "s_dist_10" char(24),
  "s_data" varchar(50),
  PRIMARY KEY ("s_w_id", "s_i_id")
);

ALTER TABLE "district" ADD FOREIGN KEY ("d_w_id") REFERENCES "warehouse" ("w_id");

ALTER TABLE "customer" ADD FOREIGN KEY ("c_w_id", "c_d_id") REFERENCES "district" ("d_w_id", "d_id");

ALTER TABLE "order" ADD FOREIGN KEY ("o_w_id", "o_d_id", "o_c_id") REFERENCES "customer" ("c_w_id", "c_d_id", "c_id");

ALTER TABLE "orderline" ADD FOREIGN KEY ("ol_w_id", "ol_d_id", "ol_o_id") REFERENCES "order" ("o_w_id", "o_d_id", "o_id");

ALTER TABLE "orderline" ADD FOREIGN KEY ("ol_i_id") REFERENCES "item" ("i_id");

ALTER TABLE "stock" ADD FOREIGN KEY ("s_w_id") REFERENCES "warehouse" ("w_id");

ALTER TABLE "stock" ADD FOREIGN KEY ("s_i_id") REFERENCES "item" ("i_id");

