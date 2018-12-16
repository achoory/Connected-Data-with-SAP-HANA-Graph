/*****************************************************/
-- #3
-- (1) Pattern matching
/*****************************************************/
-- Find the co-authors of Fred
-- We load authors into our graph
-- Nodes first:
SELECT "ID", "TYPE", "NAME" FROM AAN.NODES WHERE TYPE = 'Author'
	INTO "HSGRA"."NODES"("ID", "TYPE", "NAME");
-- Edges next:
SELECT "SOURCE", "TARGET", 'isAuthoredBy' AS "TYPE" FROM AAN.EDGES WHERE "TYPE" = 'author'
	INTO "HSGRA"."EDGES"("SOURCE", "TARGET", "TYPE");

-- So, now we have two types of nodes in our graph: papers and authors
SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY "TYPE" ORDER BY "ID" ) AS "RANK" FROM "HSGRA"."NODES")
	WHERE RANK < 10;
SELECT "TYPE", COUNT(*) AS C FROM "HSGRA"."NODES" GROUP BY "TYPE";	
-- ... and we have a new edge type
SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY "TYPE" ORDER BY "_ID" ) AS RANK FROM "HSGRA"."EDGES")
	WHERE RANK < 10;	
SELECT "TYPE", COUNT(*) AS C FROM "HSGRA"."EDGES" GROUP BY "TYPE";	


-- common "NAME" column
UPDATE "HSGRA"."NODES" SET "NAME" = "TITLE" WHERE "TYPE" = 'Paper';	

-- Full-text index
CREATE FULLTEXT INDEX "HSGRA"."FTI_NODES_NAME" ON "HSGRA"."NODES"("NAME")
	SEARCH ONLY OFF FAST PREPROCESS ON;
SELECT * FROM "SYS"."M_FULLTEXT_QUEUES" WHERE SCHEMA_NAME = 'HSGRA';
SELECT * FROM "HSGRA"."NODES" WHERE CONTAINS(*, 'fred');

/*
--> check graph meta
--> DB explorer

-- Query 1:
	MATCH (paper)-[e1]->(author_1)
	WHERE SYS.TEXT_CONTAINS(author_1.NAME, 'fred richards', 'FUZZY(0.8)')
	AND paper.TYPE = 'Paper' AND author_1.TYPE = 'Author'
	RETURN paper.ID AS ID, e1.TYPE AS TYPE, author_1.NAME AS NAME
-- similar
	MATCH (paper)-[e1]->(author_1)
	WHERE SYS.TEXT_CONTAINS(paper.NAME, 'spoke hub', 'FUZZY(1)')
	AND paper.TYPE = 'Paper' AND author_1.TYPE = 'Author'
	RETURN paper.ID AS ID, e1.TYPE AS TYPE, author_1.NAME AS NAME

-- Query 2:
	MATCH (paper)-[e1]->(author_1), (paper)-[e2]->(author_2) 
	WHERE SYS.TEXT_CONTAINS(author_1.NAME, 'fred richards', 'FUZZY(0.8)') 
	AND author_1.TYPE = 'Author'  
	AND paper.TYPE = 'Paper' 
	AND author_2.TYPE = 'Author' 
	RETURN author_1.ID AS ID_A1, paper.ID AS ID_P, author_2.ID AS ID_A2

-- show authors who worked on language with some franz/k
	MATCH (paper)-[e1]->(author_1), (paper)-[e2]->(author_2) 
	WHERE ( SYS.TEXT_CONTAINS(paper.NAME, 'langauge', 'FUZZY(0.8)') AND SYS.TEXT_CONTAINS(author_1.NAME, 'franz', 'FUZZY(0.8)') )
	AND author_1.TYPE = 'Author' 
	AND paper.TYPE = 'Paper' 
	AND author_2.TYPE = 'Author' 
	RETURN author_1.ID AS ID_A1, paper.ID AS ID_P, author_2.ID AS ID_A2
*/



-- Creating edges for co-authors.
-- Data manipulation is handled via SQL.
DROP CALCULATION SCENARIO "HSGRA"."CYPHER" CASCADE;
CREATE CALCULATION SCENARIO "HSGRA"."CYPHER" USING '
<?xml version="1.0"?>
<cubeSchema version="2" operation="createCalculationScenario" defaultLanguage="en">
  <calculationScenario schema="HSGRA" name="CYPHER">
    <calculationViews>
      <graph name="match_subgraphs_node" defaultViewFlag="true"
      schema="HSGRA" workspace="GRAPH" action="MATCH_SUBGRAPHS">
        <expression>
          <![CDATA[$$openCypher$$]]>
        </expression>
        <viewAttributes>
            <viewAttribute name="ID_A1" datatype="string"/>
            <viewAttribute name="ID_P"  datatype="string"/>
            <viewAttribute name="ID_A2" datatype="string"/>
        </viewAttributes>
      </graph>
    </calculationViews>
    <variables>
		<variable name="$$openCypher$$" type="graphVariable"/>
    </variables>
  </calculationScenario>
</cubeSchema>
' WITH PARAMETERS ('EXPOSE_NODE'=('match_subgraphs_node', 'CYPHER'));

-- Finding co-authors, using CYPHER via SQL
SELECT ID_A1 AS "SOURCE", ID_A2 AS "TARGET", "ID_P", 'co-author' AS "TYPE"
	FROM "HSGRA"."CYPHER" (		placeholder."$$openCypher$$" => 'MATCH (P)-[e1]->(A1), (P)-[e2]->(A2) WHERE A1.TYPE = ''Author'' AND e1.TYPE = ''isAuthoredBy'' AND P.TYPE = ''Paper'' AND e2.TYPE = ''isAuthoredBy'' AND A2.TYPE = ''Author'' RETURN A1.ID AS ID_A1, P.ID AS ID_P, A2.ID AS ID_A2'
);

-- Finding co-authors, then add aggregation.
-- Finding co-authors and the number of papers on which they collaborated.
SELECT "SOURCE", "TARGET", "TYPE", COUNT(*) AS "COU" FROM (
	SELECT ID_A1 AS "SOURCE", ID_A2 AS "TARGET", "ID_P", 'co-author' AS "TYPE"
		FROM "HSGRA"."CYPHER" (
			placeholder."$$openCypher$$" => 'MATCH (P)-[e1]->(A1), (P)-[e2]->(A2) WHERE A1.TYPE = ''Author'' AND e1.TYPE = ''isAuthoredBy'' AND P.TYPE = ''Paper'' AND e2.TYPE = ''isAuthoredBy'' AND A2.TYPE = ''Author'' RETURN A1.ID AS ID_A1, P.ID AS ID_P, A2.ID AS ID_A2'
		)
) GROUP BY "SOURCE", "TARGET", "TYPE"
ORDER BY COU DESC
;

/*****************************************************/
-- #3
-- (2) Creating "co-author" edges
/*****************************************************/
INSERT INTO HSGRA.EDGES ("SOURCE", "TARGET", "TYPE", "COU")
SELECT "SOURCE", "TARGET", "TYPE", COUNT(*) AS "COU" FROM (
	SELECT ID_A1 AS "SOURCE", ID_A2 AS "TARGET", "ID_P", 'co-author' AS "TYPE"
		FROM "HSGRA"."CYPHER" (
			placeholder."$$openCypher$$" => 'MATCH (P)-[e1]->(A1), (P)-[e2]->(A2) WHERE A1.TYPE = ''Author'' AND e1.TYPE = ''isAuthoredBy'' AND P.TYPE = ''Paper'' AND e2.TYPE = ''isAuthoredBy'' AND A2.TYPE = ''Author'' RETURN A1.ID AS ID_A1, P.ID AS ID_P, A2.ID AS ID_A2'
		)
) GROUP BY "SOURCE", "TARGET", "TYPE"
;
ALTER TABLE HSGRA.EDGES ALTER ("COU" BIGINT);

--> checking graph model to examine changes

/*****************************************************/
-- #3
-- (4) views
/*****************************************************/
-- Creating new graphs, using views.

-- AUTHOR graphs for reference and co-author
DROP VIEW HSGRA.V_NODES_AUTHOR;
CREATE VIEW HSGRA.V_NODES_AUTHOR AS (
	SELECT * FROM HSGRA.NODES WHERE "TYPE" = 'Author'
);
DROP VIEW HSGRA.V_EDGES_AUTHOR_CO;
CREATE VIEW HSGRA.V_EDGES_AUTHOR_CO AS (
	SELECT * FROM HSGRA.EDGES WHERE "TYPE" = 'co-author'
);
DROP GRAPH WORKSPACE "HSGRA"."GRAPH_AUTHOR_CO";
CREATE GRAPH WORKSPACE "HSGRA"."GRAPH_AUTHOR_CO"
	EDGE TABLE "HSGRA"."V_EDGES_AUTHOR_CO"
		SOURCE COLUMN "SOURCE"
		TARGET COLUMN "TARGET"
		KEY COLUMN "_ID"
	VERTEX TABLE "HSGRA"."V_NODES_AUTHOR" 
		KEY COLUMN "ID";

-- PAPER graph for citation
DROP VIEW HSGRA.V_NODES_PAP;
CREATE VIEW HSGRA.V_NODES_PAP AS (
	SELECT * FROM HSGRA.NODES WHERE "TYPE" = 'Paper'
);
DROP VIEW HSGRA.V_EDGES_PAP;
CREATE VIEW HSGRA.V_EDGES_PAP AS (
	SELECT * FROM HSGRA.EDGES WHERE "TYPE" = 'citation'
);
DROP GRAPH WORKSPACE "HSGRA"."GRAPH_PAPER";
CREATE GRAPH WORKSPACE "HSGRA"."GRAPH_PAPER"
	EDGE TABLE "HSGRA"."V_EDGES_PAP"
		SOURCE COLUMN "SOURCE"
		TARGET COLUMN "TARGET"
		KEY COLUMN "_ID"
	VERTEX TABLE "HSGRA"."V_NODES_PAP" 
		KEY COLUMN "ID";







