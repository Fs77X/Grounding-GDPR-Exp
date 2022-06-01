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

  CREATE user_policy(
    id character varying(50) NOT NULL,
    querier character varying(255) NOT NULL,
    purpose character varying(255) NOT NULL,
    ttl integer NOT NULL,
    origin character varying(255) NOT NULL,
    objection character varying(255) NOT NULL,
    sharing character varying(255) NOT NULL,
    enforcement_action character varying(255),
    inserted_at character varying(255) NOT NULL,
    tomb integer NOT NULL,
    device_id integer NOT NULL,
  )
  create index tomb_index on user_policy (tomb);

CREATE TABLE usertable(id character varying(50) NOT NULL,
  shop_name character varying(20) NOT NULL,
  obs_date character varying(255) NOT NULL,
  obs_time character varying(255) NOT NULL,
  user_interest character varying(20),
  device_id integer NOT NULL,
  tomb integer NOT NULL
  );

  create index tomb_index on usertable (tomb);


  create table user_policy(id character varying(50) NOT NULL,  querier character varying(255) NOT NULL,
  purpose character varying(255) NOT NULL,
  ttl integer NOT NULL,
  origin character varying(255) NOT NULL,
  objection character varying(255) NOT NULL,
  sharing character varying(255) NOT NULL,
  enforcement_action character varying(255),
  inserted_at character varying(255) NOT NULL, tomb integer, device_id integer);

create index tomb_index on user_policy (tomb);



  -- {DEC=dec1, USR=user9993, SRC=src3, OBJ=obj93, CAT=cat3, ACL=acl3, Data=99931754682999311113468279993-448309203999316143130999993-6183550329993-477178749993-167714670099936, PUR=purpose18, SHR=shr3, TTL=14000}
