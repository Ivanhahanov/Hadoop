## PIG
```
products = LOAD '/data/products' using PigStorage (',') AS (id:int, name:chararray, weight:float, price:float);
orders = LOAD '/data/orders' using PigStorage (',') AS (id:int, city:chararray, email:chararray, name:chararray, phone:chararray); 
joins = JOIN orders BY id, products BY id;
dump joins

output: 
(1,msc,alice@mail.com,Alice,82013008850,1,teapot,41.2,20.0)
(1,msc,alice@mail.com,Alice,82013008850,1,cup,21.1,10.0)
(2,spb,bob@mail.com,Bob,83879303040,2,spoon,5.1,5.0)
(3,ekb,carol@mail.com,Carol,84692179832,3,plate,15.0,15.0)
(3,ekb,carol@mail.com,Carol,84692179832,3,spoon,5.1,5.0)                                                               
(3,ekb,carol@mail.com,Carol,84692179832,3,cup,21.1,10.0)
```

## HIVE

```
create table orders(id int, city string, email string, name string, phone string)
insert into orders(id, city, email, name, phone) values 
    (1,"msc","alice@mail.com","Alice","82013008850"),
    (2,"spb","bob@mail.com","Bob","83879303040");
    (3,"ekb","carol@mail.com","Carol","84692179832");

create table products(order_id int, name string, weight float, price float);
insert into products(order_id, name, weight, price) values 
    (1,"cup","21.1","10"),
    (1,"teapot","41.2","20"),
    (2,"spoon","5.1","5"),
    (3,"cup","21.1","10"),
    (3,"spoon","5.1","5"),
    (3,"plate","15.0","15");

select * from orders o join products p ON (o.id = p.order_id);
Output:
2021-04-12 15:14:06     End of local task; Time Taken: 2.79 sec.
+-------+---------+-----------------+---------+--------------+-------------+---------+-----------+----------+
| o.id  | o.city  |     o.email     | o.name  |   o.phone    | p.order_id  | p.name  | p.weight  | p.price  |
+-------+---------+-----------------+---------+--------------+-------------+---------+-----------+----------+
| 1     | msc     | alice@mail.com  | Alice   | 82013008850  | 1           | cup     | 21.1      | 10.0     |
| 1     | msc     | alice@mail.com  | Alice   | 82013008850  | 1           | teapot  | 41.2      | 20.0     |
| 2     | spb     | bob@mail.com    | Bob     | 83879303040  | 2           | spoon   | 5.1       | 5.0      |
| 3     | ekb     | carol@mail.com  | Carol   | 84692179832  | 3           | cup     | 21.1      | 10.0     |
| 3     | ekb     | carol@mail.com  | Carol   | 84692179832  | 3           | spoon   | 5.1       | 5.0      |
| 3     | ekb     | carol@mail.com  | Carol   | 84692179832  | 3           | plate   | 15.0      | 15.0     |
+-------+---------+-----------------+---------+--------------+-------------+---------+-----------+----------+ 
```

## MapReduce
*in Hadoop/relations*
```
cat ../database/* | go run mapper.go | sort | go run reduser.go

Output:
1|msc|alice@mail.com|Alice|82013008850|cup|21.1|10
1|msc|alice@mail.com|Alice|82013008850|teapot|41.2|20
2|spb|bob@mail.com|Bob|83879303040|spoon|5.1|5
3|ekb|carol@mail.com|Carol|84692179832|cup|21.1|10
3|ekb|carol@mail.com|Carol|84692179832|plate|15.0|15
3|ekb|carol@mail.com|Carol|84692179832|spoon|5.1|5
```
