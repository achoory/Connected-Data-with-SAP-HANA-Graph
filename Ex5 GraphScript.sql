/*****************************************************/
-- #5
-- (1) GraphScript
/*****************************************************/
-- Look at the data again
SELECT * FROM (	SELECT *, RANK() OVER (PARTITION BY "TYPE" ORDER BY "ID" ) AS RANK FROM HSGRA.NODES ) WHERE RANK < 6;
SELECT * FROM ( SELECT *, RANK() OVER (PARTITION BY "TYPE" ORDER BY "_ID" ) AS RANK FROM HSGRA.EDGES )	WHERE RANK < 6;	


/*****************************************************/
-- #5
-- Neighborhood in GraphScript
/*****************************************************/
DROP TYPE "HSGRA"."TT_NODES" CASCADE;
CREATE TYPE "HSGRA"."TT_NODES" AS TABLE ("ID" NVARCHAR(5000), "NAME" NVARCHAR(5000), "TYPE" NVARCHAR(5000));

DROP PROCEDURE "HSGRA"."GS_NEI";
CREATE OR REPLACE PROCEDURE "HSGRA"."GS_NEI"(
	IN startV NVARCHAR(5000), 
	IN minDepth INTEGER, 
	IN maxDepth INTEGER, 
	OUT res "HSGRA"."TT_NODES")
LANGUAGE GRAPH READS SQL DATA AS
BEGIN
  GRAPH g = Graph("HSGRA","GRAPH");
  VERTEX v_s = Vertex(:g, :startV);
  MULTISET<VERTEX> ms_n = Neighbors(:g, :v_s, :minDepth, :maxDepth);
  res = SELECT :v."ID", :v."NAME", :v."TYPE" FOREACH v IN :ms_n;
END;

CALL "HSGRA"."GS_NEI"('6841', 3, 3, ?);

-- Consumption via SQLScript
DROP FUNCTION "HSGRA"."F_NEI";
CREATE OR REPLACE FUNCTION "HSGRA"."F_NEI"(	IN startV NVARCHAR(5000), IN minDepth INTEGER, IN maxDepth INTEGER )
    RETURNS "HSGRA"."TT_NODES"
LANGUAGE SQLSCRIPT READS SQL DATA AS
BEGIN
    CALL "HSGRA"."GS_NEI"(:startV, :minDepth, :maxDepth, RESULT);
    RETURN :RESULT;
END;

SELECT * FROM HSGRA.F_NEI('6841', 1, 10);

-- What's the "spatial reach" of Fred?
SELECT ST_CONCAVEHULLAGGR(LOC_3857).ST_TRANSFORM(4326).ST_ASGEOJSON() AS "GEOM" FROM (
SELECT * FROM HSGRA.F_NEI('6841', 0, 10) AS R
	LEFT JOIN HSGRA.NODES AS N ON R.ID = N.ID
);


/*****************************************************/
-- #5
-- Shortest Path
/*****************************************************/
DROP TYPE "HSGRA"."TT_EDGES" CASCADE;
CREATE TYPE "HSGRA"."TT_EDGES" AS TABLE ("_ID" BIGINT, "SOURCE" NVARCHAR(5000), "TARGET" NVARCHAR(5000), "TYPE" NVARCHAR(5000), "COU" BIGINT, "i" BIGINT);
DROP TYPE "HSGRA"."TT_NODES" CASCADE;
CREATE TYPE "HSGRA"."TT_NODES" AS TABLE ("ID" NVARCHAR(5000), "NAME" NVARCHAR(5000), "TYPE" NVARCHAR(5000), "i" BIGINT);

DROP PROCEDURE "HSGRA"."GS_SP";
CREATE OR REPLACE PROCEDURE "HSGRA"."GS_SP"(
	IN startV NVARCHAR(5000), IN targetV NVARCHAR(5000), 
	OUT o_len BIGINT, 
	OUT o_edges HSGRA.TT_EDGES, 
	OUT o_nodes HSGRA.TT_NODES
	)
LANGUAGE GRAPH READS SQL DATA AS
BEGIN
  GRAPH g = Graph("HSGRA","GRAPH_AUTHOR_CO");
  VERTEX v_s = Vertex(:g, :startV);
  VERTEX v_t = Vertex(:g, :targetV);
  WeightedPath<BIGINT> p = Shortest_Path(:g, :v_s, :v_t);
  o_len = LENGTH(:p);
  Sequence<EDGE> s_e = Edges(:p);
  Sequence<VERTEX> s_v = Vertices(:p);
  o_edges = SELECT :e."_ID", :e."SOURCE", :e."TARGET", :e."TYPE", :e."COU", :i FOREACH e IN :s_e WITH ORDINALITY AS i;
  o_nodes = SELECT :v."ID", :v."NAME", :v."TYPE", :i FOREACH v IN :s_v WITH ORDINALITY AS i;
END;

CALL HSGRA.GS_SP('6841', '3790', ?, ?, ?);
-- Can be wrapped in a similar way in a SQLScript procedure/function itself

-- shortest path in authors with 1/cou as distance
SELECT * FROM ( SELECT *, RANK() OVER (PARTITION BY "TYPE" ORDER BY "_ID" ) AS RANK FROM HSGRA.EDGES )	WHERE RANK < 6;	

DROP PROCEDURE HSGRA.GS_SP_DIST;
CREATE OR REPLACE PROCEDURE HSGRA.GS_SP_DIST(
	IN startV NVARCHAR(5000), 
	IN targetV NVARCHAR(5000), 
	OUT o_weight DOUBLE, 
	OUT o_edges HSGRA.TT_EDGES,
	OUT o_nodes HSGRA.TT_NODES
	)
LANGUAGE GRAPH READS SQL DATA AS
BEGIN
  GRAPH g_all = Graph("HSGRA","GRAPH");
  MULTISET<Edge> edges = e IN Edges(:g_all) WHERE :e."TYPE" == N'co-author';
  GRAPH g = Subgraph(:g_all, :edges);
  VERTEX v_s = Vertex(:g, :startV);
  VERTEX v_t = Vertex(:g, :targetV);
  WeightedPath<DOUBLE> p = Shortest_Path(:g, :v_s, :v_t, (Edge e) => DOUBLE { return DOUBLE(1)/DOUBLE(:e."COU"); } );
  o_weight = WEIGHT(:p);
  Sequence<EDGE> s_e = Edges(:p);
  Sequence<VERTEX> s_v = Vertices(:p);
  o_edges = SELECT :e."_ID", :e."SOURCE", :e."TARGET", :e."TYPE", :e."COU", :i FOREACH e IN :s_e WITH ORDINALITY AS i;
  o_nodes = SELECT :v."ID", :v."NAME", :v."TYPE", :i FOREACH v IN :s_v WITH ORDINALITY AS i;
END;

-- compare hop distance and 1/COU distance
CALL "HSGRA"."GS_SP"('2724', '4051', ?, ?, ?);
CALL "HSGRA"."GS_SP_DIST"('2724', '4051', ?, ?, ?);


/***************************************/
-- Shortest Path - scalar UDF
DROP PROCEDURE "HSGRA"."GS_SP_DIST_SCALAR";
CREATE OR REPLACE PROCEDURE "HSGRA"."GS_SP_DIST_SCALAR"(
	IN startV NVARCHAR(5000), 
	IN targetV NVARCHAR(5000), 
	OUT o_dist BIGINT
	)
LANGUAGE GRAPH READS SQL DATA AS
BEGIN
  GRAPH g = Graph("HSGRA","GRAPH_AUTHOR_CO");
  Vertex v1 = Vertex(:g, :startV);
  Vertex v2 = Vertex(:g, :targetV);
  WeightedPath<BIGINT> p = Shortest_Path(:g, :v1, :v2);
  BIGINT dist = LENGTH(:p);
  IF (:dist < 1L) { o_dist = -1L; }
  ELSE {o_dist = :dist; }
END;
--CALL "HSGRA"."GS_SP_DIST_SCALAR"('2724', '4051', ?);

DROP FUNCTION "HSGRA"."F_SP_DIST_SCALAR";
CREATE OR REPLACE FUNCTION "HSGRA"."F_SP_DIST_SCALAR"(IN startV NVARCHAR(5000), IN targetV NVARCHAR(5000))
	RETURNS DIST BIGINT
LANGUAGE SQLSCRIPT READS SQL DATA AS
BEGIN
    IF :startV = :targetV THEN DIST = 0;
    ELSE CALL "HSGRA"."GS_SP_DIST_SCALAR"(:startV, :targetV, DIST);
    END IF;
END;

SELECT HSGRA.F_SP_DIST_SCALAR('2724', '4051') AS "HOP_DIST" FROM DUMMY LIMIT 1;
--SELECT HSGRA.F_SP_DIST_SCALAR('2724', '2724') AS "HOP_DIST" FROM DUMMY LIMIT 1;-- hop dist to self = 0
--SELECT HSGRA.F_SP_DIST_SCALAR('1831', '2724') AS "HOP_DIST" FROM DUMMY LIMIT 1;-- hop dist to not reach = 0


-- pairwise distance between two set of nodes: Freds and Franzs
SELECT * FROM (
	SELECT L.ID AS ID_L, L.NAME AS NAME_L, R.ID AS ID_R, R.NAME AS NAME_R, HSGRA.F_SP_DIST_SCALAR(L."ID", R."ID") AS DIST
	FROM 
		(SELECT ID, NAME FROM HSGRA.NODES WHERE CONTAINS(NAME, 'fred') AND "TYPE" = 'Author' LIMIT 20) AS L, 
		(SELECT ID, NAME FROM HSGRA.NODES WHERE CONTAINS(NAME, 'franz') AND "TYPE" = 'Author' LIMIT 20) AS R 
	)
	ORDER BY DIST DESC
;


/*****************************************************/
-- #5
-- Breadth First Search
/*****************************************************/
DROP TYPE "HSGRA"."TT_NODES" CASCADE;
CREATE TYPE "HSGRA"."TT_NODES" AS TABLE ("ID" NVARCHAR(5000), "LEVEL_FWD" BIGINT, "NAME" NVARCHAR(5000), "TYPE" NVARCHAR(5000));

DROP PROCEDURE "HSGRA"."GS_BFS";
CREATE OR REPLACE PROCEDURE "HSGRA"."GS_BFS"(IN startV NVARCHAR(5000), OUT res "HSGRA"."TT_NODES")
LANGUAGE GRAPH READS SQL DATA AS
BEGIN
  GRAPH g = Graph("HSGRA","GRAPH_PAPER");
  ALTER g ADD TEMPORARY VERTEX ATTRIBUTE (Bigint "LEVEL_FWD" = -1L);
  VERTEX v_s = Vertex(:g, :startV);
  TRAVERSE BFS :g FROM :v_s
    ON VISIT VERTEX (Vertex v, BigInt lvl) {
        v.LEVEL_FWD = :lvl;
    };
  Multiset<Vertex> ms_v = v in Vertices(:g) WHERE :v."LEVEL_FWD" > 0L;
  res = SELECT :v."ID", :v."LEVEL_FWD", :v."NAME", :v."TYPE" FOREACH v IN :ms_v;
END;

-- Citation Graph #tada!
CALL HSGRA.GS_BFS('H94-1009', ?);




	
	

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
