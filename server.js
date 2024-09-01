require('dotenv').config();
const express = require('express');
const { Client } = require('pg'); 
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = 3002;
const ALLOWED_SIGNERS_FILE = path.join(__dirname, 'allowedsigner.txt');

let allowedSigners = {};

// PostgreSQL connection configuration
const client = new Client({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: parseInt(process.env.DB_PORT, 10),
});

// Connect to PostgreSQL
client.connect();

// Function to create the allowedsigner.txt file if it does not exist
const initializeAllowedSignersFile = () => {
  if (!fs.existsSync(ALLOWED_SIGNERS_FILE)) {
    fs.writeFileSync(ALLOWED_SIGNERS_FILE, '', 'utf-8');
  }
};

// Load the allowed signers from the file
const loadAllowedSigners = () => {
  allowedSigners = {};
  if (fs.existsSync(ALLOWED_SIGNERS_FILE)) {
    const data = fs.readFileSync(ALLOWED_SIGNERS_FILE, 'utf-8');
    data.split('\n').forEach(line => {
      const [signerAddress, expiryTimestamp] = line.split(',');
      if (signerAddress && expiryTimestamp) {
        allowedSigners[signerAddress.toLowerCase()] = parseInt(expiryTimestamp, 10);
      }
    });
  }
};

// Initialize files
initializeAllowedSignersFile();

// Load allowed signers initially
loadAllowedSigners();

// Periodically reload allowed signers every 5 minutes
setInterval(loadAllowedSigners, 300000);

app.get('/', async (req, res) => {
  const signerAddress = req.query.address;
  const errorAfter = parseInt(req.query.timeout || '45', 10) * 60; // Convert minutes to seconds

  if (!signerAddress) {
    return res.status(400).json({ error: 'address is required' });
  }

  const currentTime = Math.floor(Date.now() / 1000);
  const addressLower = signerAddress.toLowerCase();

  // Check if signer_address is whitelisted
  if (!allowedSigners[addressLower]) {
    return res.status(404).json({
      status: 'error',
      message: 'Signer address not whitelisted, contact @matroxdev on Discord to be whitelisted'
    });
  }

  const expiryTimestamp = allowedSigners[addressLower];
  if (currentTime > expiryTimestamp) {
    // Reload the allowed signers in case it has changed
    loadAllowedSigners();
    if (!allowedSigners[addressLower]) {
      return res.status(404).json({
        status: 'error',
        message: 'Signer address not whitelisted'
      });
    }
  }

  // Check timestamp validity in the PostgreSQL database
  try {
    const result = await client.query(
      'SELECT MAX(timestamp) AS lastTimestamp FROM block WHERE signer_address = $1',
      [addressLower]
    );

    const row = result.rows[0];
    if (row && row.lasttimestamp) {
      const lastTimestamp = row.lasttimestamp;
      if (currentTime - lastTimestamp > errorAfter) {
        return res.status(400).json({
          status: 'error',
          message: `Signer did not validate block in the last ${errorAfter / 60} minutes`
        });
      }
      return res.status(200).json({ status: 'ok', last_validated_timestamp: lastTimestamp });
    } else {
      return res.status(404).json({
        status: 'error',
        message: 'Signer address not found in database'
      });
    }
  } catch (err) {
    console.error('Database error:', err);
    return res.status(500).json({ error: 'Database error' });
  }
});

// Start the server
app.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});
