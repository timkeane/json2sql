const fs = require('fs');
require('dotenv').config();
const protocol = process.env.PROTOCOL;
const host = process.env.HOST;
const port = process.env.PORT;
const url = `${protocol}://${host}:${port}/geoserver/carto/ows?service=WFS&version=1.0.0&request=GetFeature&outputFormat=application%2Fjson&typeName=carto:`;

const layers = [
  'LANDMASSFRINGE_SDO',
  'LANDMASSPANGAEAWET_SDO'
];

const outDir = './sql/';

fs.rmSync(outDir, {recursive: true, force: true});
fs.mkdirSync(outDir);

function dropTable(table, sql) {
  fs.appendFileSync(sql, `DROP TABLE IF EXISTS ${table};\n\n`);
}

function createTable(table, feature, sql) {
  let create = `CREATE TABLE ${table} (\n`;
  const properties = feature.properties;
  for (const prop in properties) {
    const type = isNaN(properties[prop]) ? 'TEXT' : 'NUMERIC';
    create += `\t${prop} ${type},\n`
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
            values.push(`'${value}'`);
          } else {
            values.push(value)
          }
        });
        const insert = `INSERT INTO ${table} VALUES (${values.join()},ST_GeomFromGeoJSON('${geom}'));\n`;
        fs.appendFileSync(sql, insert);
      });
      fs.appendFileSync(sql, `\nUPDATE ${table} SET geom = ST_SetSRID(geom,2263);\n`);
      fs.appendFileSync(sql, `\nCREATE INDEX ${table}_geom_idx ON ${table} USING GIST (geom);\n`); 
    });
  });
});
