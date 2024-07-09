require('dotenv').config();
const fs = require('fs');

const protocol = process.env.PROTOCOL;
const host = process.env.HOST;
const port = process.env.PORT;
const namespace = process.env.NAMESPACE;
const outDir = process.env.OUT;
const url = `${protocol}://${host}:${port}/geoserver/${namespace}/ows?service=WFS&version=1.0.0&request=GetFeature&outputFormat=application%2Fjson&typeName=${namespace}:`;

const layers = JSON.parse(process.env.LAYERS);

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

if (!fs.existsSync('./sql')) {
  fs.mkdirSync('./sql');
}
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
    const type = typeof properties[prop] === 'number' ? 'NUMERIC' : 'TEXT';
    create += `\t${column} ${type},\n`
  }
  create += '\tgeom GEOMETRY\n);\n\n';
  fs.appendFileSync(sql, create);
}

function getTableName(layer) {
  if (typeof layer === 'string') {
    return layer.replace(/_SDO/, '');
  }
  return layer.name;
}

function getJsonUrl(layer) {
  if (typeof layer === 'string') {
    return `${url}${layer}`;
  }
  return layer.url;
}

layers.forEach(layer => {
  const table = getTableName(layer);
  const jsonUrl = getJsonUrl(layer);
  const sql = `${outDir}${table}.sql`;
  fs.appendFileSync(`${outDir}all.sql`, `\\i ${table}.sql;\n`); 
  console.log(`fetching ${jsonUrl}`);
  fetch(jsonUrl).then(response => {
    response.json().then(geojson => {
      const features = geojson.features;
      dropTable(table, sql);
      createTable(table, features[0], sql);
      features.forEach(feature => {
        const geom = JSON.stringify(feature.geometry);
        const properties = feature.properties;
        const values = [];
        Object.values(properties).forEach(value => {
          if (typeof value === 'number') {
            values.push(value);
          } else {
            if (value === undefined || value === null || value === '') {
              values.push('NULL');
            } else {
              values.push(`'${(value + '').replace(/'/g, "''")}'`);
            }
          }
        });
        const insert = `INSERT INTO ${table} VALUES (${values.join()},ST_GeomFromGeoJSON('${geom}'));\n`;
        fs.appendFileSync(sql, insert);
      });
      fs.appendFileSync(sql, `\nUPDATE ${table} SET geom = ST_SetSRID(geom,2263);\n`);
      fs.appendFileSync(sql, `\nCREATE INDEX ${table}_geom_idx ON ${table} USING GIST (geom);\n`); 
      fs.appendFileSync(sql, `\nCREATE VIEW ${table}_vw AS SELECT * FROM ${table};\n`); 
    }).catch(err => {
      console.error(err);
    });
  });
});
