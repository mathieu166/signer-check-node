const axios = require('axios');
const sqlite3 = require('sqlite3').verbose();
const fs = require('fs');
const path = require('path');

const DB_PATH = path.join(__dirname, 'signers.db');
const RPC_URL = 'https://rpc.vitruveo.xyz/';


// Get the maximum block number from the database
const getMaxBlockNumber = async () => {
  return new Promise((resolve, reject) => {
    const db = new sqlite3.Database(DB_PATH);
    db.get('SELECT MAX(block_number) AS maxBlockNumber FROM signer', (err, row) => {
      if (err) reject(err);
      resolve(row.maxBlockNumber);
    });
    db.close();
  });
};

// Update the signer in the database
const updateSigner = async (signerAddress, blockNumber, timestamp) => {
  return new Promise((resolve, reject) => {
    const db = new sqlite3.Database(DB_PATH);
    db.run(`
      INSERT INTO signer (signer_address, block_number, timestamp)
      VALUES (?, ?, ?)
      ON CONFLICT(signer_address) DO UPDATE SET
        block_number = excluded.block_number,
        timestamp = excluded.timestamp
    `, [signerAddress.toLowerCase(), blockNumber, timestamp], (err) => {
      if (err) reject(err);
      resolve();
    });
    db.close();
  });
};

// Fetch the current block number
const fetchBlockNumber = async () => {
  const response = await axios.post(RPC_URL, {
    id: 1,
    jsonrpc: "2.0",
    method: "eth_blockNumber",
    params: []
  });
  return parseInt(response.data.result, 16);
};

// Fetch block info by block number
const fetchBlockInfo = async (blockNumber) => {
  const response = await axios.post(RPC_URL, {
    id: 1,
    jsonrpc: "2.0",
    method: "eth_getBlockByNumber",
    params: [`0x${blockNumber.toString(16)}`, false]
  });
  return response.data.result;
};

// Fetch the signer for a block hash
const fetchSigner = async (blockHash) => {
  const response = await axios.post(RPC_URL, {
    id: 1,
    jsonrpc: "2.0",
    method: "clique_getSigner",
    params: [blockHash]
  });
  return response.data.result;
};

// Process the blocks
const processBlocks = async () => {
  while (true) {
    try {
      const currentBlockNumber = await fetchBlockNumber();
      const latestBlockProcessed = (await getMaxBlockNumber() || (currentBlockNumber - 500));
      
      const maxBlockToProcess = currentBlockNumber - 10;

      if (maxBlockToProcess <= latestBlockProcessed) {
        console.log("No new blocks to process or all blocks are too close to the latest block");
        await new Promise(resolve => setTimeout(resolve, 10000)); // Sleep before checking again
        continue;
      }

      for (let blockNumber = latestBlockProcessed; blockNumber <= maxBlockToProcess; blockNumber++) {
        try {
          const blockInfo = await fetchBlockInfo(blockNumber);
          const blockHash = blockInfo.hash;
          const timestamp = parseInt(blockInfo.timestamp, 16);

          if (blockHash) {
            const signerAddress = await fetchSigner(blockHash);
            if (signerAddress) {
              await updateSigner(signerAddress, blockNumber, timestamp);
            }
          }
        } catch (error) {
          console.error(`Error fetching block data: ${error.message}`);
          //await new Promise(resolve => setTimeout(resolve, 5000)); // Retry delay
        }
        await new Promise(resolve => setTimeout(resolve, 1000 / 10)); // Rate limit
      }
    } catch (error) {
      console.error(`Error fetching latest block number: ${error.message}`);
      await new Promise(resolve => setTimeout(resolve, 5000)); // Retry delay
    }
  }
};

// Start processing blocks
processBlocks();
