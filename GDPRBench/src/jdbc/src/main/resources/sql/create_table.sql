-- Copyright (c) 2015 YCSB contributors. All rights reserved.
--
-- Licensed under the Apache License, Version 2.0 (the "License"); you
-- may not use this file except in compliance with the License. You
-- may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
-- implied. See the License for the specific language governing
-- permissions and limitations under the License. See accompanying
-- LICENSE file.

-- Creates a Table.

-- Drop the table if it exists;
DROP TABLE IF EXISTS usertable;

-- Create the user table with 5 fields.
CREATE TABLE usertable(YCSB_KEY VARCHAR PRIMARY KEY, USR character(100),
  DEC character(100), SRC character(100),
  OBJ character(100), CAT character(100),
  ACL character(100), Data character(1000),
  PUR character(100), SHR character(100),
  TTL character(10000));

CREATE TABLE usertable(id character varying(50) NOT NULL,
  shop_name character varying(20) NOT NULL,
  obs_date character varying(255) NOT NULL,
  obs_time character varying(255) NOT NULL,
  user_interest character varying(20),
  device_id integer NOT NULL,
  querier character varying(255) NOT NULL,
  purpose character varying(255) NOT NULL,
  ttl integer NOT NULL,
  origin character varying(255) NOT NULL,
  objection character varying(255) NOT NULL,
  sharing character varying(255) NOT NULL,
  enforcement_action character varying(255),
  inserted_at character varying(255) NOT NULL
  );
  -- {DEC=dec1, USR=user9993, SRC=src3, OBJ=obj93, CAT=cat3, ACL=acl3, Data=99931754682999311113468279993-448309203999316143130999993-6183550329993-477178749993-167714670099936, PUR=purpose18, SHR=shr3, TTL=14000}
create policy controller_all on usertable for select using(true);
CREATE POLICY controller_all ON usertable TO controller USING (true) WITH CHECK (true);
create policy controller_insert on usertable for insert to controller with check(true);
create policy controller_update on usertable for update to controller using(true) with check(true);
create policy controller_delete on usertable for delete to controller using(true);
create policy controller_update on usertable for update to controller using(true);

-- works, not sure how else to do row level policies
CREATE POLICY admin_controller ON usertable TO controller USING (true) WITH CHECK (true);
GRANT SELECT, INSERT, UPDATE, DELETE ON usertable TO controller;
CREATE POLICY admin_processor ON usertable TO processor USING (true) WITH CHECK (true);
GRANT SELECT, INSERT, UPDATE, DELETE ON usertable TO processor;
CREATE POLICY admin_customer ON usertable TO customer USING (true) WITH CHECK (true);
GRANT SELECT, INSERT, UPDATE, DELETE ON usertable TO customer;


CREATE POLICY controller_insert on usertable for insert with check(true);

CREATE POLICY controller_update on usertable for update USING (true) WITH CHECK (true);

CREATE POLICY controller_delete on usertable for delete USING (true);
