require('dotenv').config(); // Load environment variables from .env file
const { Client } = require('pg'); // Import PostgreSQL client
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

// PostgreSQL connection configuration from environment variables
const client = new Client({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: parseInt(process.env.DB_PORT, 10),
  });

const CSV_FILE_PATH = path.join(__dirname, 'blocks.csv');
const MAX_BLOCK = process.env.MAX_BLOCK;

// Connect to PostgreSQL
client.connect();

// Insert block data into the PostgreSQL table
const insertBlock = async (blockNumber, timestamp, hash, signerAddress, transactionCount, feeEarned) => {
	try {
		await client.query(
			`
      INSERT INTO block (block_number, timestamp, hash, signer_address, transaction_count, fee_earned)
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT(block_number) DO UPDATE SET
        timestamp = EXCLUDED.timestamp,
        hash = EXCLUDED.hash,
        signer_address = EXCLUDED.signer_address,
        transaction_count = EXCLUDED.transaction_count,
        fee_earned = EXCLUDED.fee_earned
    `,
			[blockNumber, timestamp, hash, signerAddress, transactionCount, feeEarned]
		);
	} catch (err) {
		console.error('Error inserting block:', err);
		throw err;
	}
};

// Process CSV file and insert data into PostgreSQL row-by-row
const processCsv = () => {
	fs.createReadStream(CSV_FILE_PATH)
		.pipe(csv())
		.on('data', async (row) => {
			const blockNumber = parseInt(row.block_number, 10);

            if(MAX_BLOCK && parseInt(MAX_BLOCK, 10) < blockNumber ){
                return;
            }
            
			const timestamp = parseInt(row.timestamp, 10);
			const hash = row.hash;
			const signerAddress = row.signer_address.toLowerCase();
			const transactionCount = parseInt(row.transaction_count, 10);
			const feeEarned = parseFloat(row.fee_earned);

			try {
				await insertBlock(blockNumber, timestamp, hash, signerAddress, transactionCount, feeEarned);
			} catch (err) {
				console.error(`Error processing block ${blockNumber}:`, err);
			}
		})
		.on('end', () => {
			console.log('CSV file processing complete.');
            //process.exit(0); // Exit the script
			//client.end(); // Close the database connection
		})
		.on('error', (err) => {
			console.error('Error reading the CSV file:', err);
			//client.end(); // Ensure the database connection is closed in case of an error
		});
};

// Start processing the CSV file
processCsv();
