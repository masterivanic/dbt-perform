INSERT INTO countries (code,name)
VALUES
  ('CM','Cameroun'),
  ('FR','France'),
  ('IT','Italie'),
  ('US','Etats Unis'),
  ('CN','Canada'),
  ('TS','Tunisie'),
  ('BR','Bresil'),
  ('JP','Japon');

INSERT INTO people (email,name,city,state, country)
VALUES
  ('luctus@protonmail.com','Jillian Foley','Gravilias','USA', 'CM'),
  ('arcu.vestibulum.ut@google.net','Jarrod Quinn','Porirua','France','FR'),
  ('gravida.sagittis.duis@aol.com','Elton Branch','Bevilacqua','Tunisie', 'TS'),
  ('tristique.ac@icloud.ca','Paki Madden','Donetsk','France', 'FR'),
  ('gravida.sit@aol.com','Brianna Kelly','Broken Arrow','Etats unis', 'US');

INSERT INTO items (name,price,weight)
VALUES
  ('Chou',3095341,97),
  ('Poulet',9575413,97),
 ('Soupe',7224189,10),
  ('Spaghetti',5416808,30),
  ('Saumon',1256262,25),
  ('Miel',1256262,25),
  ('Baguette',1256262,25),
  ('Epinards',1256262,25);

INSERT INTO invoices (person_id,payment_date,payment_info,subtotal,shipping,total,country,address,ship_date, ship_info)
VALUES
  (5,'Feb 2, 2023','vulputate mauris sagittis placerat.',9,5,8,'FR','P.O. Box 112, 2793 Duis Rd.','Jun 14, 2023','velit eu sem. Pellentesque ut ipsum ac mi eleifend egestas.'),
  (4,'Feb 6, 2023','sagittis. Nullam vitae diam.',8,9,4,'US','P.O. Box 201, 8468 Mi Rd.','May 9, 2024','imperdiet non, vestibulum nec, euismod in, dolor. Fusce feugiat. Lorem'),
  (3,'May 23, 2024','velit. Cras lorem lorem, luctus ut, pellentesque',4,5,1,'IT','Ap #855-7252 Ut St.','Aug 20, 2023','molestie in, tempus eu, ligula. Aenean euismod mauris eu elit.'),
  (2,'May 8, 2023','cursus luctus, ipsum leo elementum sem,',3,8,1,'JP','8026 Mauris St.','Jan 31, 2024','tincidunt. Donec vitae erat vel pede blandit congue. In scelerisque'),
  (1,'Jul 20, 2024','tempor lorem, eget mollis lectus pede et risus. Quisque libero',3,5,1,'CN','P.O. Box 130, 152 Justo Road','Apr 26, 2024','a, dui. Cras pellentesque. Sed dictum. Proin eget odio. Aliquam');

INSERT INTO lineitems(invoice_id,item_id,quantity,price)
VALUES
    (3,1,20,120),
    (4,8,10,120),
    (2,6,3,120),
    (1,4,4,120),
    (5,5,50,120);

INSERT INTO shipchart(country,weight,cost)
VALUES
    ('FR', 12, 500),
    ('FR', 10, 512),
    ('TS', 4, 12),
    ('BR', 3, 120),
    ('JP', 2, 5);
