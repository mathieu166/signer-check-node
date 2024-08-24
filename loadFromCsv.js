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
const MAX_BLOCK = process.env.MAX_BLOCK ? parseInt(process.env.MAX_BLOCK, 10) : null;
const BATCH_SIZE = 1000; // Adjust this value as needed based on your memory constraints

// Connect to PostgreSQL
client.connect();

// Insert a batch of blocks into the PostgreSQL table
const insertBatch = async (batch) => {
	const queryText = `
      INSERT INTO block (block_number, timestamp, hash, signer_address, transaction_count, fee_earned)
      VALUES ${batch.map((_, i) => `($${i * 6 + 1}, $${i * 6 + 2}, $${i * 6 + 3}, $${i * 6 + 4}, $${i * 6 + 5}, $${i * 6 + 6})`).join(',')}
      ON CONFLICT(block_number) DO UPDATE SET
        timestamp = EXCLUDED.timestamp,
        hash = EXCLUDED.hash,
        signer_address = EXCLUDED.signer_address,
        transaction_count = EXCLUDED.transaction_count,
        fee_earned = EXCLUDED.fee_earned
    `;

	// Flatten the batch array for parameterized query
	const queryParams = batch.flat();

	try {
		await client.query(queryText, queryParams);
	} catch (err) {
		console.error('Error executing batch insert:', err);
		throw err;
	}
};

// Process CSV file and insert data into PostgreSQL in batches
const processCsv = () => {
	const stream = fs.createReadStream(CSV_FILE_PATH).pipe(csv());
	let batch = [];
	let reachedMaxBlock = false;

	stream
		.on('data', (row) => {
			if (reachedMaxBlock || !row || !row.block_number) {
				return;
			}

			const blockNumber = parseInt(row.block_number, 10);

			if (MAX_BLOCK && blockNumber > MAX_BLOCK) {
				reachedMaxBlock = true;
				// Insert the remaining batch and stop processing
				if (batch.length > 0) {
					stream.pause();
					insertBatch(batch)
						.then(() => {
							console.log('Reached MAX_BLOCK; processing halted.');
							client.end(); // Close the database connection
							process.exit(0); // Stop the process after inserting the final batch
						})
						.catch((err) => {
							console.error('Error inserting final batch before MAX_BLOCK:', err);
							client.end();
							process.exit(1); // Exit with error status if there's an issue
						});
				} else {
					console.log('Reached MAX_BLOCK; no batch to process.');
					client.end(); // Close the database connection
					process.exit(0); // Stop the process immediately
				}
				return;
			}

			const timestamp = parseInt(row.timestamp, 10);
			const hash = row.hash;
			const signerAddress = row.signer_address.toLowerCase();
			const transactionCount = parseInt(row.transaction_count, 10);
			const feeEarned = parseFloat(row.fee_earned);

			// Add the row to the batch
			batch.push([blockNumber, timestamp, hash, signerAddress, transactionCount, feeEarned]);

			// If the batch is full, insert it and clear the batch
			if (batch.length >= BATCH_SIZE) {
				stream.pause(); // Pause reading the CSV file
				insertBatch(batch)
					.then(() => {
						batch = []; // Clear the batch after insertion
						stream.resume(); // Resume reading after batch is inserted
					})
					.catch((err) => {
						console.error('Error inserting batch:', err);
						stream.resume(); // Resume reading even if there's an error to avoid blocking
					});
			}
		})
		.on('end', async () => {
			// Insert any remaining rows in the last batch only if MAX_BLOCK wasn't reached
			if (!reachedMaxBlock && batch.length > 0) {
				try {
					await insertBatch(batch);
				} catch (err) {
					console.error('Error inserting final batch:', err);
				}
			}
			console.log('CSV file processing complete.');
			client.end(); // Close the database connection
		})
		.on('error', (err) => {
			console.error('Error reading the CSV file:', err);
			client.end(); // Ensure the database connection is closed in case of an error
			process.exit(1); // Exit with error status if there's an issue reading the file
		});
};

// Start processing the CSV file
processCsv();
