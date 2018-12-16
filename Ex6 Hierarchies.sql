/*****************************************************/
-- #6
-- (1) Using Hierarchies in SAP HANA
/*****************************************************/
-- the data
SELECT * FROM "HSGRA"."I_TOPICS" ;

SELECT t.node_id, count(*) as c FROM "HSGRA"."I_TOPICS" AS T
	LEFT JOIN "AAN"."I_PAPER_IDS" AS P
	ON PAPER_ID LIKE '%'||RIGHT(T.TRUE_ID, 2)||'%' 
	group by t.node_id;

-- Hierarchy generation function
SELECT * FROM HIERARCHY (
	SOURCE ( SELECT "PARENT_ID", "NODE_ID", "NAME", "TRUE_ID" FROM "HSGRA"."I_TOPICS" )
	SIBLING ORDER BY "NODE_ID"
);
SELECT * FROM HIERARCHY (
	SOURCE "HSGRA"."I_TOPICS" 
	SIBLING ORDER BY "NODE_ID"
);

DROP VIEW "HSGRA"."V_PC_HIER";
CREATE VIEW "HSGRA"."V_PC_HIER" AS SELECT * FROM HIERARCHY (
	SOURCE ( SELECT "PARENT_ID", "NODE_ID", "NAME", "TRUE_ID", LENGTH("NAME") AS LEN FROM "HSGRA"."I_TOPICS" )
	SIBLING ORDER BY "NODE_ID"
	CACHE FORCE
);
SELECT * FROM "HSGRA"."V_PC_HIER";

SELECT * FROM HIERARCHY_DESCENDANTS (
    SOURCE "HSGRA"."V_PC_HIER"
    START WHERE "NODE_ID" = '3'
    DISTANCE FROM 1 );

SELECT * FROM HIERARCHY_ANCESTORS (
    SOURCE "HSGRA"."V_PC_HIER"
    START WHERE "NODE_ID" = '744'
);

SELECT * FROM HIERARCHY_SIBLINGS (
    SOURCE "HSGRA"."V_PC_HIER"
    START WHERE "NODE_ID" = '743'
);

--LEFT JOIN "AAN"."I_PAPER_IDS" AS P ON PAPER_ID LIKE '%'||RIGHT(T.TRUE_ID, 2)||'%' 
	
SELECT * FROM HIERARCHY_DESCENDANTS_AGGREGATE (
	SOURCE "HSGRA"."V_PC_HIER"
	JOIN "AAN"."I_PAPER_IDS" ON "PAPER_ID" LIKE '%'||RIGHT("TRUE_ID", 2)||'%'
	MEASURES (
		COUNT("AAN"."I_PAPER_IDS"."PAPER_ID") AS "COU"
	)
	WHERE "HIERARCHY_LEVEL" <= 3

);













