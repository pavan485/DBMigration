const { Client } = require('pg');
const fs = require('fs');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

const originalClient = new Client({
  user: 'old',
  host: 'localhost',
  database: 'old',
  password: 'hehehe',
  port: 5432,
});

const migratedClient = new Client({
  user: 'new',
  host: 'localhost',
  database: 'new',
  password: 'hahaha',
  port: 5433,
});

// Function to compare data for accounts table

async function compareAccountsTable() {
  const originalQuery = `SELECT * FROM accounts`;
  const migratedQuery = `SELECT * FROM accounts`;

  const originalData = await originalClient.query(originalQuery);
  const migratedData = await migratedClient.query(migratedQuery);

  const missingRecords = [];
  const corruptedRecords = [];
  const newRecords = [];

  const migratedDataMap = new Map();
  migratedData.rows.forEach(row => {
    migratedDataMap.set(row.id, row);
  });

  originalData.rows.forEach(originalRow => {
    const migratedRow = migratedDataMap.get(originalRow.id);

    if (!migratedRow) {
      missingRecords.push(originalRow);
    } else {
      const isCorrupted = Object.entries(originalRow).some(([key, value]) => value !== migratedRow[key]);
      if (isCorrupted) {
        corruptedRecords.push({ original: originalRow, migrated: migratedRow });
      }
      migratedDataMap.delete(originalRow.id);
    }
  });

  migratedDataMap.forEach((value) => newRecords.push(value));

  return { missingRecords, corruptedRecords, newRecords };
}

// Function to write into csv files
async function generateReport() {
  await originalClient.connect();
  await migratedClient.connect();

  const { missingRecords, corruptedRecords, newRecords } = await compareAccountsTable();

  const missingCsvWriter = createCsvWriter({
    path: 'missing_records.csv',
    header: Object.keys(missingRecords[0] || {}).map(key => ({ id: key, title: key })),
  });
  await missingCsvWriter.writeRecords(missingRecords);

  const corruptedCsvWriter = createCsvWriter({
    path: 'corrupted_records.csv',
    header: ['Original', 'Migrated'],
  });
  const corruptedRecordsFormatted = corruptedRecords.map(record => ({
    Original: JSON.stringify(record.original),
    Migrated: JSON.stringify(record.migrated),
  }));
  await corruptedCsvWriter.writeRecords(corruptedRecordsFormatted);

  
  const newCsvWriter = createCsvWriter({
    path: 'new_records.csv',
    header: Object.keys(newRecords[0] || {}).map(key => ({ id: key, title: key })),
  });
  await newCsvWriter.writeRecords(newRecords);

  await originalClient.end();
  await migratedClient.end();
}

generateReport().catch(err => console.error('Error generating report:', err));
