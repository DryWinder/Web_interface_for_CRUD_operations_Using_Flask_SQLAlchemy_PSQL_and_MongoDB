INSERT INTO locations(regname, areaname, tername, tertypename)
	SELECT DISTINCT regname, areaname, tername, tertypename FROM zno_records;

CREATE INDEX idx_locations_regname_areaname_tername
ON locations (regname, areaname, tername);






