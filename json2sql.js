require('dotenv').config();
const fs = require('fs');

const protocol = process.env.PROTOCOL;
const host = process.env.HOST;
const port = process.env.PORT;
const namespace = process.env.NAMESPACE;
const url = `${protocol}://${host}:${port}/geoserver/carto/ows?service=WFS&version=1.0.0&request=GetFeature&outputFormat=application%2Fjson&typeName=${namespace}:`;

const layers = [
  'BACKGROUND_SDO',
  // 'BOARDWALK_SDO',
  'BOROUGH_BORDER_SDO',
  'BOROUGH_POINT_SDO',
  // 'BUILDING_SDO',
  // 'CENTERLINE_SDO',
  'HYDRO_LABELLINE_SDO',
  'HYDRO_LABELPOINT_SDO',
  'HYDRO_LABELPOLY_SDO',
  'LANDMASSFRINGE_SDO',
  'LANDMASSPANGAEAWET_SDO',
  'LOW_RES_ROAD_SDO',
  // 'MARSH_SDO',
  // 'MEDIAN_SDO',
  'NEIGHBORHOOD_POINT_SDO',
  // 'OPEN_SPACE_NO_PARK_SDO',
  // 'PARKING_LOT_SDO',
  // 'PARK_LABEL_SDO',
  // 'PARK_SDO',
  // 'PAVEMENT_EDGE2D_SDO',
  // 'PLAZA_SDO',
  // 'RAILROAD_BRIDGE_SDO',
  // 'RAILROAD_ELEVATED_SDO',
  // 'RAILROAD_PASSENGER_SDO',
  // 'RAILROAD_STRUCTURE_SDO',
  // 'ROADBED_SDO',
  'ROAD_NATEARTH_SDO',
  'ROAD_TIGER_SDO',
  // 'SIDEWALK_SDO',
  // 'TOLL_STRUCT_SDO',
  // 'TRANSPORT_STRUCTURE_SDO',
  // 'TUNNEL_SDO'
];

const outDir = './sql/';

const reserved = [
  'ALL',
  'ANALYSE',
  'ANALYZE',
  'AND',
  'ANY',
  'ASC',
  'ASYMMETRIC',
  'AUTHORIZATION',
  'BINARY',
  'BOTH',
  'CASE',
  'CAST',
  'CHECK',
  'COLLATE',
  'COLLATION',
  'COLUMN',
  'CONCURRENTLY',
  'CONSTRAINT',
  'CROSS',
  'CURRENT_CATALOG',
  'CURRENT_DATE',
  'CURRENT_ROLE',
  'CURRENT_SCHEMA',
  'CURRENT_TIME',
  'CURRENT_TIMESTAMP',
  'CURRENT_USER',
  'DEFAULT',
  'DEFERRABLE',
  'DESC',
  'DISTINCT',
  'DO',
  'ELSE',
  'END',
  'FALSE',
  'FOREIGN',
  'FREEZE',
  'FULL',
  'ILIKE',
  'IN',
  'INITIALLY',
  'INNER',
  'IS',
  'ISNULL',
  'JOIN',
  'LATERAL',
  'LEADING',
  'LEFT',
  'LIKE',
  'LOCALTIME',
  'LOCALTIMESTAMP',
  'NATURAL',
  'NOT',
  'NOTNULL',
  'NULL',
  'ONLY',
  'OR',
  'OUTER',
  'OVERLAPS',
  'PLACING',
  'PRIMARY',
  'REFERENCES',
  'RIGHT',
  'SELECT',
  'SESSION_USER',
  'SIMILAR',
  'SOME',
  'SYMMETRIC',
  'SYSTEM_USER',
  'TABLE',
  'TABLESAMPLE',
  'THEN',
  'TRAILING',
  'TRUE',
  'UNIQUE',
  'USER',
  'USING',
  'VARIADIC',
  'VERBOSE',
  'WHEN'
];

fs.rmSync(outDir, {recursive: true, force: true});
fs.mkdirSync(outDir);

function dropTable(table, sql) {
  fs.appendFileSync(sql, `DROP VIEW IF EXISTS ${table}_vw;\n\n`);
  fs.appendFileSync(sql, `DROP TABLE IF EXISTS ${table};\n\n`);
}

function createTable(table, feature, sql) {
  let create = `CREATE TABLE ${table} (\n`;
  const properties = feature.properties;
  for (const prop in properties) {
    const column = reserved.indexOf(prop.toUpperCase()) === -1 ? prop : `${prop}_`;
    const type = isNaN(properties[prop]) ? 'TEXT' : 'NUMERIC';
    create += `\t${column} ${type},\n`
  }
  create += '\tgeom GEOMETRY\n);\n\n';
  fs.appendFileSync(sql, create);
}

layers.forEach(layer => {
  const table = layer.replace(/_SDO/, '');
  const sql = `${outDir}${table}.sql`;
  fs.appendFileSync(`${outDir}all.sql`, `\\i ${sql};\n`); 
  fetch(`${url}${layer}`).then(response => {
    response.json().then(geojson => {
      const features = geojson.features;
      dropTable(table, sql);
      createTable(table, features[0], sql);
      features.forEach(feature => {
        const geom = JSON.stringify(feature.geometry);
        const properties = feature.properties;
        const values = [];
        Object.values(properties).forEach(value => {
          if (isNaN(value)) {
            values.push(`'${value.replace(/'/, "''")}'`);
          } else {
            if (value !== 0 && !value) {
              values.push('NULL');
            } else {
              values.push(value);
            }
          }
        });
        const insert = `INSERT INTO ${table} VALUES (${values.join()},ST_GeomFromGeoJSON('${geom}'));\n`;
        fs.appendFileSync(sql, insert);
      });
      fs.appendFileSync(sql, `\nUPDATE ${table} SET geom = ST_SetSRID(geom,2263);\n`);
      fs.appendFileSync(sql, `\nCREATE INDEX ${table}_geom_idx ON ${table} USING GIST (geom);\n`); 
      fs.appendFileSync(sql, `\nCREATE VIEW ${table}_vw AS SELECT * FROM ${table};\n`); 
    });
  });
});
