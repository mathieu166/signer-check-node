require('dotenv').config(); // Load environment variables from .env file
const axios = require('axios');
const fs = require('fs');
const path = require('path');

const RPC_URL = process.env.RPC_URL || 'http://localhost:8545';
const CSV_FILE_PATH = path.join(__dirname, 'blocks.csv');
const START_BLOCK = 71;
const WRITE_QTY=10;
// Fetch block info by block number
const fetchBlockInfo = async (blockNumber) => {
	try {
		const response = await axios.post(RPC_URL, {
			id: 1,
			jsonrpc: '2.0',
			method: 'eth_getBlockByNumber',
			params: [`0x${blockNumber.toString(16)}`, true], // Set to `true` to include transactions
		});
		return response.data.result;
	} catch (err) {
		console.error('Error fetching block info:', err);
		throw err;
	}
};

// Fetch the current gas price
const fetchGasPrice = async () => {
	try {
		const response = await axios.post(RPC_URL, {
			id: 1,
			jsonrpc: '2.0',
			method: 'eth_gasPrice',
			params: [],
		});
		return parseInt(response.data.result, 16);
	} catch (err) {
		console.error('Error fetching gas price:', err);
		throw err;
	}
};

// Fetch the signer for a block hash
const fetchSigner = async (blockHash) => {
	try {
		const response = await axios.post(RPC_URL, {
			id: 1,
			jsonrpc: '2.0',
			method: 'clique_getSigner',
			params: [blockHash],
		});
		return response.data.result;
	} catch (err) {
		console.error('Error fetching signer:', err);
		throw err;
	}
};

// Append block data to CSV
const appendToCsv = (data) => {
	const fileExists = fs.existsSync(CSV_FILE_PATH);
	const header = 'block_number,timestamp,hash,signer_address,transaction_count,fee_earned\n';

	if (!fileExists) {
		fs.writeFileSync(CSV_FILE_PATH, header);
	}else{
        //fs.appendFileSync(CSV_FILE_PATH, '\n');
    }

	const csvData = data.map(block => 
		`${block.blockNumber},${block.timestamp},${block.hash},${block.signerAddress},${block.transactionCount},${block.feeEarned}`
	).join('\n') + '\n';

	fs.appendFileSync(CSV_FILE_PATH, csvData);
};

// Process the blocks and write to CSV
const processBlocks = async () => {
	let gasPrice;
	let blocksToWrite = [];
	
	for (let blockNumber = START_BLOCK; ; blockNumber++) {
		try {
			if (!gasPrice) {
				gasPrice = await fetchGasPrice(); // Fetch the current gas price
			}

			const blockInfo = await fetchBlockInfo(blockNumber);
			const blockHash = blockInfo.hash;
			const timestamp = parseInt(blockInfo.timestamp, 16);
			const transactionCount = blockInfo.transactions.length;

			// Calculate fee earned
			const gasUsed = parseInt(blockInfo.gasUsed, 16);
			const feeEarned = gasUsed * gasPrice;

			if (blockHash) {
				const signerAddress = await fetchSigner(blockHash);
				if (signerAddress) {
					blocksToWrite.push({
						blockNumber,
						timestamp,
						hash: blockHash,
						signerAddress: signerAddress.toLowerCase(),
						transactionCount,
						feeEarned
					});

					// Write to CSV every 1000 blocks
					if (blocksToWrite.length >= WRITE_QTY) {
						appendToCsv(blocksToWrite);
						blocksToWrite = []; // Reset the buffer
					}
				}
			}

			await new Promise(resolve => setTimeout(resolve, 1000 / 100)); // Rate limit
		} catch (error) {
			console.error(`Error processing block ${blockNumber}: ${error.message}`);
			await new Promise(resolve => setTimeout(resolve, 5000)); // Retry delay
		}
	}
};

// Start processing blocks
processBlocks();