const express = require('express');
const axios = require('axios');
const cheerio = require('cheerio');
const cors = require('cors');
const { Client } = require('pg');  // PostgreSQL client

const app = express();
app.use(cors());

const BASE_URL = 'https://srv129.services.gc.ca/ei_regions/eng/';
const POSTAL_CODE_ENDPOINT = 'postalcode.aspx?_code=';
const THIRTY_DAYS_IN_MS = 30 * 24 * 60 * 60 * 1000;  // 30 days in milliseconds

// PostgreSQL Client setup
const client = new Client({
    connectionString: process.env.DATABASE_URL,  // Provided by Fly.io
    ssl: {
        rejectUnauthorized: false  // Necessary for Fly.io's PostgreSQL setup
    }
});

client.connect();

// Create tables if they don't exist
client.query(`
    CREATE TABLE IF NOT EXISTS postal_code_data (
        postal_code TEXT PRIMARY KEY,
        census_subdivision_name TEXT,
        common_name TEXT,
        census_division_name TEXT,
        ei_economic_region_name TEXT,
        ei_economic_region_url TEXT,
        date_retrieved TIMESTAMPTZ
    );
`);

client.query(`
    CREATE TABLE IF NOT EXISTS economic_region_data (
        economic_region_name TEXT PRIMARY KEY,
        province TEXT,
        economic_region_code TEXT,
        unemployment_rate TEXT,
        insured_hours_required TEXT,
        min_weeks_payable TEXT,
        max_weeks_payable TEXT,
        best_weeks_required TEXT,
        date_retrieved TIMESTAMPTZ
    );
`);

// Helper function to check if data is more than 30 days old
const isDataStale = (retrievalDate) => {
    return (Date.now() - new Date(retrievalDate).getTime()) > THIRTY_DAYS_IN_MS;
};

// Function to scrape the economic region details page
const scrapeEconomicRegion = async (relativeURL) => {
    const url = `${BASE_URL}${relativeURL}`;
    console.log(`Fetching economic region data from: ${url}`);

    const { data: html } = await axios.get(url);
    console.log('Successfully fetched economic region page.');

    const $ = cheerio.load(html);
    const tableID = 'regions';
    const tableData = {};

    $(`#${tableID} tbody tr`).each((i, row) => {
        const cells = $(row).find('td');
        tableData.Province = $(cells[0]).text().trim();
        tableData.EconomicRegionCode = $(cells[1]).text().trim();
        tableData.EconomicRegionName = $(cells[2]).text().trim();
        tableData.UnemploymentRate = $(cells[3]).text().trim();
        tableData.InsuredHoursRequired = $(cells[4]).text().trim();
        tableData.MinWeeksPayable = $(cells[5]).text().trim();
        tableData.MaxWeeksPayable = $(cells[6]).text().trim();
        tableData.BestWeeksRequired = $(cells[7]).text().trim();
    });

    console.log('Scraped economic region data:', tableData);

    return tableData;
};

// Scrape the table based on postal code
app.get('/scrape', async (req, res) => {
    const postalCode = req.query.postalCode;
    console.log(`Received postal code: ${postalCode}`);

    // Check if postal code data is in the database and still fresh
    client.query(`SELECT * FROM postal_code_data WHERE postal_code = $1`, [postalCode], async (err, result) => {
        if (err) {
            console.error('Database error:', err);
            return res.status(500).json({ error: 'Database error' });
        }

        const postalRow = result.rows[0];

        // If postal data is found and fresh, return it
        if (postalRow && !isDataStale(postalRow.date_retrieved)) {
            console.log('Using cached postal code data.');
            postalRow.EconomicRegionDetails = await getEconomicRegionData(postalRow.ei_economic_region_name, postalRow.ei_economic_region_url);
            return res.json({ data: [postalRow] });
        }

        console.log('Fetching postal code data from:', `${BASE_URL}${POSTAL_CODE_ENDPOINT}${postalCode.replace(' ', '')}`);

        try {
            // Scrape postal code data
            const { data: html } = await axios.get(`${BASE_URL}${POSTAL_CODE_ENDPOINT}${postalCode.replace(' ', '')}`);
            const $ = cheerio.load(html);
            const tableID = 'table';
            const row = $(`#${tableID} tbody tr`).first();
            const cells = row.find('td');

            const postalData = {
                PostalCode: $(cells[0]).text().trim(),
                CensusSubdivisionName: $(cells[1]).text().trim(),
                CommonName: $(cells[2]).text().trim(),
                CensusDivisionName: $(cells[3]).text().trim(),
                EIEconomicRegionName: $(cells[4]).find('a').text().trim(),
                EIEconomicRegionURL: $(cells[4]).find('a').attr('href'),
                date_retrieved: new Date()
            };

            // Insert or update postal code data in the database
            client.query(
                `INSERT INTO postal_code_data (postal_code, census_subdivision_name, common_name, census_division_name, ei_economic_region_name, ei_economic_region_url, date_retrieved)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (postal_code) DO UPDATE
                 SET census_subdivision_name = EXCLUDED.census_subdivision_name,
                     common_name = EXCLUDED.common_name,
                     census_division_name = EXCLUDED.census_division_name,
                     ei_economic_region_name = EXCLUDED.ei_economic_region_name,
                     ei_economic_region_url = EXCLUDED.ei_economic_region_url,
                     date_retrieved = EXCLUDED.date_retrieved`,
                [postalCode, postalData.CensusSubdivisionName, postalData.CommonName, postalData.CensusDivisionName, postalData.EIEconomicRegionName, postalData.EIEconomicRegionURL, postalData.date_retrieved]
            );

            // Fetch economic region data
            postalData.EconomicRegionDetails = await getEconomicRegionData(postalData.EIEconomicRegionName, postalData.EIEconomicRegionURL);

            return res.json({ data: [postalData] });
        } catch (error) {
            console.error('Error scraping postal code page:', error);
            return res.status(500).json({ error: 'Error scraping postal code data' });
        }
    });
});

// Helper function to get or scrape economic region data
const getEconomicRegionData = (regionName, regionURL) => {
    return new Promise((resolve, reject) => {
        // Check if economic region data is in the database and still fresh
        client.query(`SELECT * FROM economic_region_data WHERE economic_region_name = $1`, [regionName], async (err, result) => {
            if (err) {
                console.error('Database error:', err);
                return reject(err);
            }

            const econRow = result.rows[0];

            // If economic region data is found and fresh, return it
            if (econRow && !isDataStale(econRow.date_retrieved)) {
                console.log('Using cached economic region data.');
                return resolve(econRow);
            }

            // Otherwise, scrape the economic region data
            console.log('Fetching economic region data from:', regionURL);
            try {
                const econRegionData = await scrapeEconomicRegion(regionURL);
                econRegionData.date_retrieved = new Date();

                // Insert or update economic region data in the database
                client.query(
                    `INSERT INTO economic_region_data (economic_region_name, province, economic_region_code, unemployment_rate, insured_hours_required, min_weeks_payable, max_weeks_payable, best_weeks_required, date_retrieved)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                     ON CONFLICT (economic_region_name) DO UPDATE
                     SET province = EXCLUDED.province,
                         economic_region_code = EXCLUDED.economic_region_code,
                         unemployment_rate = EXCLUDED.unemployment_rate,
                         insured_hours_required = EXCLUDED.insured_hours_required,
                         min_weeks_payable = EXCLUDED.min_weeks_payable,
                         max_weeks_payable = EXCLUDED.max_weeks_payable,
                         best_weeks_required = EXCLUDED.best_weeks_required,
                         date_retrieved = EXCLUDED.date_retrieved`,
                    [regionName, econRegionData.Province, econRegionData.EconomicRegionCode, econRegionData.UnemploymentRate, econRegionData.InsuredHoursRequired, econRegionData.MinWeeksPayable, econRegionData.MaxWeeksPayable, econRegionData.BestWeeksRequired, econRegionData.date_retrieved]
                );

                return resolve(econRegionData);
            } catch (error) {
                console.error('Error scraping economic region page:', error);
                return reject(error);
            }
        });
    });
};

// Start the server
const PORT = process.env.PORT || 8000;
app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
});