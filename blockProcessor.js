require('dotenv').config(); // Load environment variables from .env file
const axios = require('axios');
const { Client } = require('pg'); // Import PostgreSQL client

// PostgreSQL connection configuration from environment variables
const client = new Client({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: parseInt(process.env.DB_PORT, 10),
});

const RPC_URL = process.env.RPC_URL;
const DEFAULT_START_BLOCK = parseInt(process.env.DEFAULT_START_BLOCK, 10) || 4834000; // Default block if not found in the database

// Connect to PostgreSQL
client.connect();

// Get the maximum block number from the database
const getMaxBlockNumber = async () => {
  try {
    const result = await client.query('SELECT MAX(block_number) AS maxBlockNumber FROM block');
    return result.rows[0].maxblocknumber || DEFAULT_START_BLOCK; // Use default if no max block number is found
  } catch (err) {
    console.error('Error getting max block number:', err);
    throw err;
  }
};

// Update the block in the database
const updateBlock = async (blockNumber, timestamp, hash, signerAddress, transactionCount, feeEarned) => {
  try {
    await client.query(`
      INSERT INTO block (block_number, timestamp, hash, signer_address, transaction_count, fee_earned)
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT(block_number) DO UPDATE SET
        timestamp = EXCLUDED.timestamp,
        hash = EXCLUDED.hash,
        signer_address = EXCLUDED.signer_address,
        transaction_count = EXCLUDED.transaction_count,
        fee_earned = EXCLUDED.fee_earned
    `, [blockNumber, timestamp, hash, signerAddress.toLowerCase(), transactionCount, feeEarned]);
  } catch (err) {
    console.error('Error updating block:', err);
    throw err;
  }
};

// Fetch the current block number
const fetchBlockNumber = async () => {
  try {
    const response = await axios.post(RPC_URL, {
      id: 1,
      jsonrpc: "2.0",
      method: "eth_blockNumber",
      params: []
    });
    return parseInt(response.data.result, 16);
  } catch (err) {
    console.error('Error fetching block number:', err);
    throw err;
  }
};

// Fetch block info by block number
const fetchBlockInfo = async (blockNumber) => {
  try {
    const response = await axios.post(RPC_URL, {
      id: 1,
      jsonrpc: "2.0",
      method: "eth_getBlockByNumber",
      params: [`0x${blockNumber.toString(16)}`, true] // Set to `true` to include transactions
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
      jsonrpc: "2.0",
      method: "eth_gasPrice",
      params: []
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
      jsonrpc: "2.0",
      method: "clique_getSigner",
      params: [blockHash]
    });
    return response.data.result;
  } catch (err) {
    console.error('Error fetching signer:', err);
    throw err;
  }
};

// Process the blocks
const processBlocks = async () => {
  let gasPrice
  while (true) {
    try {
      if(!gasPrice){
          gasPrice = await fetchGasPrice(); // Fetch the current gas price
      }

      const currentBlockNumber = parseInt(await fetchBlockNumber(), 10);
      const latestBlockProcessed = parseInt(await getMaxBlockNumber(), 10);

      if (isNaN(currentBlockNumber) || isNaN(latestBlockProcessed)) {
        throw new Error('Failed to parse block numbers');
      }
      
      const maxBlockToProcess = currentBlockNumber - 10;

      if (maxBlockToProcess <= latestBlockProcessed) {
        console.log("No new blocks to process or all blocks are too close to the latest block");
        await new Promise(resolve => setTimeout(resolve, 10000)); // Sleep before checking again
        continue;
      }


      for (let blockNumber = latestBlockProcessed + 1; blockNumber <= maxBlockToProcess; blockNumber++) {
        try {
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
              await updateBlock(blockNumber, timestamp, blockHash, signerAddress, transactionCount, feeEarned);
            }
          }
        } catch (error) {
          console.error(`Error fetching block data: ${error.message}`);
        }
        await new Promise(resolve => setTimeout(resolve, 1000 / 100)); // Rate limit
      }
    } catch (error) {
      console.error(`Error fetching latest block number: ${error.message}`);
      await new Promise(resolve => setTimeout(resolve, 5000)); // Retry delay
    }
  }
};

// Start processing blocks
processBlocks();
