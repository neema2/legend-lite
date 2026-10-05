-- The parties of Studio's demo model (studio/demo/projects/party: demo::party::PartyDB), for DuckDB in the tab: what a
-- query on the party project, opened by name from Depot, reads. One statement per line.
CREATE SCHEMA IF NOT EXISTS PARTY;
CREATE OR REPLACE TABLE PARTY.PARTY (ID INTEGER PRIMARY KEY, NAME VARCHAR(200), COUNTRY VARCHAR(2));
INSERT INTO PARTY.PARTY VALUES (1, 'Meridian Capital', 'US'), (2, 'Halberd Securities', 'GB'), (3, 'Kestrel Partners', 'JP'), (4, 'Northgate Asset Management', 'US'), (5, 'Banque Lumière', 'FR');
