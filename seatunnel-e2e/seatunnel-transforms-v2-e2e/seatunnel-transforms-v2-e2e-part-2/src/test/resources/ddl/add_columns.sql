--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  shop
-- ----------------------------------------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS `shop`;
use shop;
INSERT INTO products
VALUES (110,"scooter","Small 2-wheel scooter",3.14),
       (111,"car battery","12V car battery",8.1),
       (112,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3",0.8),
       (113,"hammer","12oz carpenter's hammer",0.75),
       (114,"hammer","14oz carpenter's hammer",0.875),
       (115,"hammer","16oz carpenter's hammer",1.0),
       (116,"rocks","box of assorted rocks",5.3),
       (117,"jacket","water resistent black wind breaker",0.1),
       (118,"spare tire","24 inch spare tire",22.2);
update products set name = 'dailai' where id = 101;
delete from products where id = 102;

alter table products ADD COLUMN add_column1 varchar(64) not null default 'yy',ADD COLUMN add_column2 int not null default 1;
update products set add_column1 = 'swm1', add_column2 = 2;

update products set name = 'dailai' where id = 110;
insert into products
values (119,"scooter","Small 2-wheel scooter",3.14,'xx',1),
       (120,"car battery","12V car battery",8.1,'xx',2),
       (121,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3",0.8,'xx',3),
       (122,"hammer","12oz carpenter's hammer",0.75,'xx',4),
       (123,"hammer","14oz carpenter's hammer",0.875,'xx',5),
       (124,"hammer","16oz carpenter's hammer",1.0,'xx',6),
       (125,"rocks","box of assorted rocks",5.3,'xx',7),
       (126,"jacket","water resistent black wind breaker",0.1,'xx',8),
       (127,"spare tire","24 inch spare tire",22.2,'xx',9);
delete from products where id = 118;

alter table products ADD COLUMN add_column3 float not null default 1.1;
update products set add_column3 = 3.3;
alter table products ADD COLUMN add_column4 timestamp not null default current_timestamp();
update products set add_column4 = current_timestamp();

delete from products where id = 113;
insert into products
values (128,"scooter","Small 2-wheel scooter",3.14,'xx',1,1.1,'2023-02-02 09:09:09'),
       (129,"car battery","12V car battery",8.1,'xx',2,1.2,'2023-02-02 09:09:09'),
       (130,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3",0.8,'xx',3,1.3,'2023-02-02 09:09:09'),
       (131,"hammer","12oz carpenter's hammer",0.75,'xx',4,1.4,'2023-02-02 09:09:09'),
       (132,"hammer","14oz carpenter's hammer",0.875,'xx',5,1.5,'2023-02-02 09:09:09'),
       (133,"hammer","16oz carpenter's hammer",1.0,'xx',6,1.6,'2023-02-02 09:09:09'),
       (134,"rocks","box of assorted rocks",5.3,'xx',7,1.7,'2023-02-02 09:09:09'),
       (135,"jacket","water resistent black wind breaker",0.1,'xx',8,1.8,'2023-02-02 09:09:09'),
       (136,"spare tire","24 inch spare tire",22.2,'xx',9,1.9,'2023-02-02 09:09:09');
update products set name = 'dailai' where id = 135;

alter table products ADD COLUMN add_column6 varchar(64) not null default 'ff' after id;
update products set add_column6 = 'swm6';

delete from products where id = 115;
insert into products
values (173,'tt',"scooter","Small 2-wheel scooter",3.14,'xx',1,1.1,'2023-02-02 09:09:09'),
       (174,'tt',"car battery","12V car battery",8.1,'xx',2,1.2,'2023-02-02 09:09:09'),
       (175,'tt',"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3",0.8,'xx',3,1.3,'2023-02-02 09:09:09'),
       (176,'tt',"hammer","12oz carpenter's hammer",0.75,'xx',4,1.4,'2023-02-02 09:09:09'),
       (177,'tt',"hammer","14oz carpenter's hammer",0.875,'xx',5,1.5,'2023-02-02 09:09:09'),
       (178,'tt',"hammer","16oz carpenter's hammer",1.0,'xx',6,1.6,'2023-02-02 09:09:09'),
       (179,'tt',"rocks","box of assorted rocks",5.3,'xx',7,1.7,'2023-02-02 09:09:09'),
       (180,'tt',"jacket","water resistent black wind breaker",0.1,'xx',8,1.8,'2023-02-02 09:09:09'),
       (181,'tt',"spare tire","24 inch spare tire",22.2,'xx',9,1.9,'2023-02-02 09:09:09');
