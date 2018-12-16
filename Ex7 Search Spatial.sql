/*****************************************************/
-- #7
-- (1) Spatial
/*****************************************************/
DROP VIEW HSGRA.V_NODES_SPATIAL;
CREATE VIEW HSGRA.V_NODES_SPATIAL AS (
	SELECT * FROM HSGRA.NODES WHERE TYPE = 'Organization' AND LOC_3857 IS NOT NULL
);

INSERT INTO HSGRA.EDGES("SOURCE", "TARGET", "TYPE", "DIST")
	SELECT L.ID AS "SOURCE", R.ID AS "TARGET", 'isCloseBy' AS "TYPE", L.LOC_3857.ST_DISTANCE(R.LOC_3857, 'meter') AS "DIST"
	FROM HSGRA.V_NODES_SPATIAL AS L, HSGRA.V_NODES_SPATIAL AS R
	WHERE L.LOC_3857.ST_WITHINDISTANCE(R.LOC_3857, 10, 'kilometer') = 1
		AND L.ID != R.ID
;
DROP VIEW HSGRA.V_EDGES_SPATIAL;
CREATE VIEW HSGRA.V_EDGES_SPATIAL AS (
	SELECT * FROM HSGRA.EDGES WHERE "TYPE" = 'isCloseBy'
);

DROP GRAPH WORKSPACE "HSGRA"."GRAPH_SPATIAL";
CREATE GRAPH WORKSPACE "HSGRA"."GRAPH_SPATIAL"
	EDGE TABLE "HSGRA"."V_EDGES_SPATIAL"
		SOURCE COLUMN "SOURCE"
		TARGET COLUMN "TARGET"
		KEY COLUMN "_ID"
	VERTEX TABLE "HSGRA"."V_NODES_SPATIAL" 
		KEY COLUMN "ID";

/*****************************************/
-- SEARCH
/*****************************************/
SELECT * FROM HSGRA.NODES;
--CREATE FULLTEXT INDEX HSGRA.FTI_NODES_NAME ON HSGRA.NODES("NAME") SEARCH ONLY OFF FAST PREPROCESS ON;

DROP VIEW HSGRA.V_ESH_NODES;
CREATE VIEW HSGRA.V_ESH_NODES AS (
	SELECT "ID", "NAME", "TYPE", "YEAR", "LOC".ST_ASGEOJSON() AS "LOC_4326"
		FROM HSGRA.NODES AS D
);
SELECT * FROM HSGRA.V_ESH_NODES WHERE CONTAINS(NAME, 'richardsn', FUZZY (0.8)) ORDER BY SCORE() DESC;
CALL ESH_CONFIG('[{
"uri":    "~/$metadata/EntitySets",
"method": "PUT",
"content":{ 
	"Fullname": "HSGRA/V_ESH_NODES",
	"EntityType": {
		"@Search.searchable": true,
		"@EnterpriseSearch.enabled": true,
		"@EnterpriseSearchHana.identifier": "HSGRA",
		"Properties": [
			{"Name": "NAME", 
				"@Search.defaultSearchElement": true, "@Search.fuzzinessThreshold": 0.8, 
				"@EnterpriseSearchHana.weight": 1.0, 
				"@EnterpriseSearch.presentationMode": [ "TITLE" ], "@EnterpriseSearch.highlighted.enabled": true},
			{"Name": "TYPE",
				"@Search.defaultSearchElement": false,
				"@EnterpriseSearch.filteringFacet.default": true, 
				"@EnterpriseSearch.filteringFacet.numberOfValues": 10,
				"@EnterpriseSearch.presentationMode": [ "SUMMARY" ]},
			{"Name": "YEAR",
				"@Search.defaultSearchElement": false,
				"@EnterpriseSearch.filteringFacet.default": true, 
				"@EnterpriseSearch.filteringFacet.numberOfValues": 10,
				"@EnterpriseSearch.presentationMode": [ "SUMMARY" ]},
			{"Name": "LOC_4326", 
				"@Search.defaultSearchElement": false, 
				"@EnterpriseSearch.presentationMode": [ "HIDDEN" ],
				"@EnterpriseSearch.usageMode": [ ]},
			{"Name": "ID", 
				"@Search.defaultSearchElement": false, "@EnterpriseSearch.presentationMode": [ "SUMMARY" ], 
				"@EnterpriseSearch.key": true}
		]
	}
}
}]',?);

CALL SYS.ESH_SEARCH('[ { "URI": [ 
	"/v5/$all?$top=10&facets=all&$filter=(Search.search(query=''SCOPE:HSGRA boston''))"
] } ]', ?);





