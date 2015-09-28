
create sequence pk_key increment 1 minvalue 1 maxvalue 9999999999 start 1 cache 1;
create sequence pk_value increment 1 minvalue 1 maxvalue 9999999999 start 1 cache 1;

create table logs(pk_key numeric(10) NOT NULL DEFAULT NEXTVAL('pk_key'::regclass),
log_name varchar(30),
server_num numeric(3),
ts timestamp,
pid numeric(10),
tid varchar(15),
sev varchar(15),
req varchar(30),
sess varchar(50),
site varchar(50),
user_name varchar(50));


create table keyv (pk_value numeric(10) NOT NULL DEFAULT NEXTVAL('pk_value'::regclass), pk_key numeric(10), "key" varchar(100),"value" text)

