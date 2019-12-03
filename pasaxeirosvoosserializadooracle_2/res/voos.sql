drop table reservasfeitas cascade constraint;
drop table voos cascade constraint;
drop table pasaxeiros cascade constraint;


create table pasaxeiros
(
dni varchar2(5),
nome varchar2(15),
telf varchar2(10),
cidade varchar2(10),
nreservas integer
);

   
insert into pasaxeiros values ('361a','luis','9861a','vigo',0);
insert into pasaxeiros values ('362b','ana','9861b','lugo',0);
insert into pasaxeiros values ('363c','pedro','9861c','lugo',0);
insert into pasaxeiros values ('364d','ana','9861d','vigo' ,0);



create table voos
(
voo integer,
orixe varchar2(15),
destino varchar2(15),
prezo integer
);

insert into voos values (1,'vigo','estambul',150);
insert into voos values (2,'estambul','vigo',200);
insert into voos values (3,'vigo','londres',80);
insert into voos values (4,'londres','vigo',90);
insert into voos values (5,'vigo','lisboa',90);
insert into voos values (6,'lisboa','vigo',100);
insert into voos values (7,'vigo','viena',200);
insert into voos values (8,'viena','vigo',250);
insert into voos values (9,'vigo','tunez',160);
insert into voos values (10,'tunez','vigo',150);
insert into voos values (11,'vigo','paris',200);
insert into voos values (12,'paris','vigo',90);

create table reservasfeitas
(
codr integer,
dni varchar2(5),
nome varchar2(15),
prezoreserva integer,
primary key(codr)
);

commit;
select * from pasaxeiros;
select * from voos;
select * from reservasfeitas;


