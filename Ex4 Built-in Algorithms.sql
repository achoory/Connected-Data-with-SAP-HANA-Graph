/*****************************************************/
-- #4
-- (1) Built-in Algorithms
/*****************************************************/
-- Additional data into graph being added: ORGANIZATIONS
-- Authors are affiliated with/to organizations.

-- NODES first:
ALTER TABLE "HSGRA"."NODES" ADD("LOC_3857" ST_POINT(3857));
ALTER TABLE "HSGRA"."NODES" ADD("LOC" ST_POINT(4326));
SELECT "ID", "TYPE", "NAME", "LOC_3857", "LOC" FROM AAN.NODES WHERE TYPE = 'Organization'
	INTO "HSGRA"."NODES"("ID", "TYPE", "NAME", "LOC_3857", "LOC");

-- EDGES next:
SELECT "SOURCE", "TARGET", 'isAffiliatedWith' AS "TYPE"	FROM AAN.EDGES WHERE "TYPE" = 'affiliation'
	INTO "HSGRA"."EDGES"("SOURCE", "TARGET", "TYPE");

-- inspect data
SELECT * FROM (	SELECT *, RANK() OVER (PARTITION BY "TYPE" ORDER BY "ID" ) AS RANK FROM "HSGRA"."NODES"	)
	WHERE RANK < 6;
SELECT "TYPE", COUNT(*) AS C FROM "HSGRA"."NODES" GROUP BY "TYPE";	

SELECT * FROM (	SELECT *, RANK() OVER (PARTITION BY "TYPE" ORDER BY "_ID" ) AS RANK FROM "HSGRA"."EDGES"	)
	WHERE RANK < 6;	
SELECT "TYPE", COUNT(*) AS C FROM "HSGRA"."EDGES" GROUP BY "TYPE";	


/*****************************************************/
-- #4
-- (2) SCC on Co-Authors
/*****************************************************/
DROP CALCULATION SCENARIO "HSGRA"."CS_SCC" CASCADE;
CREATE CALCULATION SCENARIO "HSGRA"."CS_SCC" USING '
<?xml version="1.0"?>
<cubeSchema version="2" operation="createCalculationScenario" defaultLanguage="en">
<calculationScenario schema="HSGRA" name="CS_SCC">
<calculationViews>
<graph name="scc_node" defaultViewFlag="true" schema="HSGRA" workspace="GRAPH_AUTHOR_CO" action="GET_STRONGLY_CONNECTED_COMPONENTS">
<expression>
</expression>
<viewAttributes>
<viewAttribute name="ID" datatype="string"/>
<viewAttribute name="COMPONENT" datatype="int"/>
</viewAttributes>
</graph>
</calculationViews>
</calculationScenario>
</cubeSchema>
' WITH PARAMETERS ('EXPOSE_NODE'=('scc_node', 'CS_SCC'));

SELECT * FROM "HSGRA"."CS_SCC" ORDER BY "COMPONENT";
SELECT COUNT(DISTINCT "COMPONENT") AS "Number of SCC" FROM "HSGRA"."CS_SCC";
SELECT "COMPONENT", COUNT(*) AS C FROM "HSGRA"."CS_SCC" GROUP BY "COMPONENT" ORDER BY C DESC;

SELECT * FROM "HSGRA"."CS_SCC" AS R
	LEFT JOIN "HSGRA"."NODES" AS N
	ON R.ID = N.ID
	WHERE R."COMPONENT" = 243
;

-- Persist SCC in NODES table
UPDATE N SET N.COMPONENT = T.COMPONENT, N.COMPONENT_SIZE = C.COMPONENT_SIZE
	FROM "HSGRA"."NODES" AS N
	INNER JOIN "HSGRA"."CS_SCC" AS T
	ON N.ID = T.ID
	INNER JOIN (SELECT "COMPONENT", COUNT(*) AS "COMPONENT_SIZE" FROM "HSGRA"."CS_SCC" GROUP BY "COMPONENT") AS C
	ON T.COMPONENT = C.COMPONENT
;
ALTER TABLE "HSGRA"."NODES" ALTER ("COMPONENT_SIZE" INTEGER);

/***************************************************/
-- GET NEIGHBORHOOD
DROP  CALCULATION SCENARIO "HSGRA"."CS_GET_NEI" CASCADE;
CREATE CALCULATION SCENARIO "HSGRA"."CS_GET_NEI" USING '
<?xml version="1.0"?>
<cubeSchema version="2" operation="createCalculationScenario" defaultLanguage="en">
<calculationScenario schema="HSGRA" name="CS_GET_NEI">
<calculationViews>
<graph name="get_neighborhood_node" defaultViewFlag="true" schema="HSGRA" workspace="GRAPH" action="GET_NEIGHBORHOOD">
<expression>
<![CDATA[{
	"parameters": {
	"startVertices": $$startVertices$$,
	"direction": "$$direction$$",
	"minDepth": $$minDepth$$,
	"maxDepth": $$maxDepth$$,
	"vertexFilter" : "$$vertexFilter$$",
	"edgeFilter" : "$$edgeFilter$$"
}
}]]>
</expression>
<viewAttributes>
<viewAttribute name="ID" datatype="string"/>
<viewAttribute name="DEPTH" datatype="int"/>
</viewAttributes>
</graph>
</calculationViews>
<variables>
<variable name="$$startVertices$$" type="graphVariable" />

<variable name="$$direction$$" type="graphVariable">
<defaultValue>outgoing</defaultValue>
</variable>
<variable name="$$minDepth$$" type="graphVariable">
<defaultValue>0</defaultValue>
</variable>
<variable name="$$maxDepth$$" type="graphVariable"/>
<variable name="$$vertexFilter$$" type="graphVariable">
<defaultValue></defaultValue>
</variable>
<variable name="$$edgeFilter$$" type="graphVariable">
<defaultValue></defaultValue>
</variable>
</variables>
</calculationScenario>
</cubeSchema>
' WITH PARAMETERS ('EXPOSE_NODE'=('get_neighborhood_node', 'CS_GET_NEI'));

-- Get Neighborhood with params
SELECT * FROM "HSGRA"."CS_GET_NEI" (
	placeholder."$$startVertices$$" => '["6841"]',
	placeholder."$$direction$$" => 'any',
	placeholder."$$minDepth$$" => '1',
	placeholder."$$maxDepth$$" => '10',
	placeholder."$$vertexFilter$$" => '',
	placeholder."$$edgeFilter$$" => ''
) --ORDER BY DEPTH DESC
;

-- Get Neighborhood with aggregation
SELECT "DEPTH", COUNT(*) AS C FROM (
	SELECT * FROM "HSGRA"."CS_GET_NEI" (
		placeholder."$$startVertices$$" => '["6841"]',
		placeholder."$$direction$$" => 'any',
		placeholder."$$maxDepth$$" => '10'
	)
)
GROUP BY DEPTH ORDER BY DEPTH ASC;

-- Get Neighborhood with aggregation and join (for TYPE)
SELECT "DEPTH", "TYPE", COUNT(*) AS C FROM (
	SELECT * FROM "HSGRA"."CS_GET_NEI" (
		placeholder."$$startVertices$$" => '["6841"]',
		placeholder."$$direction$$" => 'any',
		placeholder."$$maxDepth$$" => '10'
	) AS R
	LEFT JOIN HSGRA.NODES AS N
	ON R.ID = N.ID
)
GROUP BY "DEPTH", "TYPE" ORDER BY "DEPTH", C DESC;







	
