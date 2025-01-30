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


alter table products drop column add_column1,drop column add_column3;
insert into products
values (146,"scooter","Small 2-wheel scooter",3.14,1),
       (147,"car battery","12V car battery",8.1,2),
       (148,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3",0.8,3),
       (149,"hammer","12oz carpenter's hammer",0.75,4),
       (150,"hammer","14oz carpenter's hammer",0.875,5),
       (151,"hammer","16oz carpenter's hammer",1.0,6),
       (152,"rocks","box of assorted rocks",5.3,7),
       (153,"jacket","water resistent black wind breaker",0.1,8),
       (154,"spare tire","24 inch spare tire",22.2,9);
update products set name = 'dailai' where id > 143;
