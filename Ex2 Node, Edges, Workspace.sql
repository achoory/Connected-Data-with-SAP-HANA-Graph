/*****************************************************/
-- #2
-- (1) The raw data as downloaded from http://tangra.cs.yale.edu/newaan/ 
/*****************************************************/
-- Nodes: Papers and Authors
SELECT "PAPER_ID", "TITLE", "YEAR" FROM "AAN"."I_PAPER_IDS" LIMIT 10;
SELECT "AUTHOR_ID", "NAME" FROM "AAN"."I_AUTHOR_IDS" LIMIT 10;

-- Edges: "citation" PAPER_ID -> PAPER_ID
SELECT * FROM "AAN"."I_ACL" LIMIT 10;

-- Edges: "isAuthoredBy" PAPER_ID -> (AUTHORS)
SELECT * FROM "AAN"."I_ACLM" LIMIT 10;

SELECT * FROM "AAN"."I_AUTHOR_AFFILIATIONS_RAW" ORDER BY "PAPER_ID" LIMIT 20;
-- SELECT * FROM "AAN"."I_AUTHOR_AFFILIATION_PAIRS";
-- SELECT * FROM "AAN"."I_PAPER_AUTHOR_AFFILIATIONS" LIMIT 10;

--> generated: ORGANIZATION (= Affiliation) and geocodes
SELECT * FROM "AAN"."G_ORGANIZATION_IDS" ORDER BY "ID" LIMIT 20;
-- PAPER_ID, AUTHOR_ID, AFFILIATION_NAME
SELECT * FROM "AAN"."G_PA_AU_AF" ORDER BY PAPER_ID;

-- PAPER_ID, FILE
SELECT * FROM "AAN"."I_PAPERS_TEXT" ORDER BY "ID" LIMIT 20;


/*****************************************************/
-- #2
-- (2) Create nodes, edges, and workspace 
/*****************************************************/
-- We  store all nodes in a single table: NODES
-- node types: papers, authors, organizations
DROP TABLE "HSGRA"."NODES" CASCADE;
CREATE TABLE "HSGRA"."NODES" AS (
	SELECT N."ID", N."TYPE", N."TITLE", N."YEAR"
	FROM AAN.NODES AS N
	WHERE N."TYPE" = 'Paper'
	);
SELECT * FROM "HSGRA"."NODES" LIMIT 20;	
SELECT COUNT(*) FROM "HSGRA"."NODES";	
ALTER TABLE "HSGRA"."NODES" ENABLE SCHEMA FLEXIBILITY;

-- We store relations in a single table: EDGES
DROP TABLE "HSGRA"."EDGES" CASCADE;
CREATE COLUMN TABLE "HSGRA"."EDGES" ( "_ID" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY NOT NULL)
	WITH SCHEMA FLEXIBILITY (DEFAULT DATA TYPE * AUTO DATA TYPE PROMOTION);
SELECT "SOURCE", "TARGET", "TYPE" 
	FROM "AAN"."EDGES" WHERE TYPE = 'citation'
	INTO "HSGRA"."EDGES" ("SOURCE", "TARGET", "TYPE")
;
SELECT * FROM "HSGRA"."EDGES" LIMIT 20;	
SELECT COUNT(*) FROM "HSGRA"."EDGES";	


/*****************************************************/
-- #2
-- (3) Consistency check and constraints
/*****************************************************/
-- Is there an identifying (unique, not null) attribute in the nodes?
SELECT "ID", COUNT(*) AS "C" FROM "HSGRA"."NODES" GROUP BY "ID" ORDER BY C DESC;

-- What about dangling edges?
SELECT COUNT(*) AS "dangling edges" FROM "HSGRA"."EDGES"
	WHERE "SOURCE" NOT IN (SELECT "ID" FROM "HSGRA"."NODES")
		OR "TARGET" NOT IN (SELECT "ID" FROM "HSGRA"."NODES");

-- If checks are ok, create primary/foreign key constraints:
ALTER TABLE "HSGRA"."NODES" ADD PRIMARY KEY ("ID");
ALTER TABLE "HSGRA"."EDGES" ALTER ("SOURCE" NVARCHAR(5000) NOT NULL 
	REFERENCES "HSGRA"."NODES" ("ID") ON UPDATE CASCADE ON DELETE CASCADE);
ALTER TABLE "HSGRA"."EDGES" ALTER ("TARGET" NVARCHAR(5000) NOT NULL 
	REFERENCES "HSGRA"."NODES" ("ID") ON UPDATE CASCADE ON DELETE CASCADE);


/*****************************************************/
-- #2
-- (4) Create workspace
/*****************************************************/
DROP GRAPH WORKSPACE "HSGRA"."GRAPH";
CREATE GRAPH WORKSPACE "HSGRA"."GRAPH"
	EDGE TABLE "HSGRA"."EDGES"
		SOURCE COLUMN "SOURCE"
		TARGET COLUMN "TARGET"
		KEY COLUMN "_ID"
	VERTEX TABLE "HSGRA"."NODES" 
		KEY COLUMN "ID";



/*****************************************************/
-- #2
-- (5) Meta graph
/*****************************************************/
DROP VIEW "HSGRA"."V_NODES_META";
CREATE VIEW "HSGRA"."V_NODES_META" AS (
	SELECT "TYPE" AS "NAME", COUNT(*) AS "COUNT" FROM "HSGRA"."NODES" GROUP BY "TYPE"
);
DROP VIEW "HSGRA"."V_EDGES_META";
CREATE VIEW "HSGRA"."V_EDGES_META" AS (
	SELECT HIERARCHY_COMPOSITE_ID(N1."TYPE", E."TYPE", N2."TYPE") AS "ID", 
			N1."TYPE" AS "SOURCE", N2."TYPE" AS "TARGET", E."TYPE" AS "TYPE", COUNT(*) AS "COUNT"
		FROM "HSGRA"."EDGES" AS E
		LEFT JOIN "HSGRA"."NODES" AS N1 ON E."SOURCE" = N1."ID"
		LEFT JOIN "HSGRA"."NODES" AS N2 ON E."TARGET" = N2."ID"
		GROUP BY N1."TYPE", N2."TYPE", E."TYPE"
);
DROP GRAPH WORKSPACE "HSGRA"."GRAPH_META";
CREATE GRAPH WORKSPACE "HSGRA"."GRAPH_META"
	EDGE TABLE "HSGRA"."V_EDGES_META"
		SOURCE COLUMN "SOURCE"
		TARGET COLUMN "TARGET"
		KEY COLUMN "ID"
	VERTEX TABLE "HSGRA"."V_NODES_META"
		KEY COLUMN "NAME";


/*****************************************************/
/* Graph measures
/*****************************************************/
-- DEGREE
DROP VIEW "HSGRA"."V_DEGREE";
CREATE VIEW "HSGRA"."V_DEGREE" AS (
	SELECT N."ID", COUNT(DISTINCT EO."_ID") AS "OUT_DEGREE", COUNT(DISTINCT EI."_ID") AS "IN_DEGREE", 
			COUNT(DISTINCT EO."_ID") + COUNT(DISTINCT EI."_ID") AS "DEGREE"
		FROM "HSGRA"."NODES" AS N
		LEFT JOIN "HSGRA"."EDGES" AS EO
			ON N."ID" = EO."SOURCE"
		LEFT JOIN "HSGRA"."EDGES" AS EI
			ON N."ID" = EI."TARGET"
		GROUP BY N."ID"
);

-- IN/OUT DEGREE
SELECT * FROM "HSGRA"."V_DEGREE" ORDER BY ID LIMIT 20;

-- DEGREE DISTRIBUTION
SELECT "DEGREE", COUNT(*) AS C FROM "HSGRA"."V_DEGREE" GROUP BY "DEGREE" ORDER BY "DEGREE";

-- AVERAGE DEGREE
SELECT "E"/"N" AS "AVG_DEGREE" FROM (
	(SELECT COUNT(*) AS "E" FROM HSGRA.EDGES) AS TE
	JOIN 
	(SELECT COUNT(*) AS "N" FROM HSGRA.NODES) AS TN ON 1=1
);

-- DENSITY
SELECT 2*"E"/("N"*("N"-1)) AS "DENSITY", "E"/("N"*("N"-1)) AS DIRECTED_DENSITY FROM (
	(SELECT COUNT(*) AS "E" FROM HSGRA.EDGES) AS TE
	JOIN 
	(SELECT COUNT(*) AS "N" FROM HSGRA.NODES) AS TN ON 1=1
);





