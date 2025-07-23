import { Client } from 'pg';
import dotenv from 'dotenv';

// Load environment variables
dotenv.config();

export const cleanedClient = new Client({
  connectionString: process.env.CLEANDB_CONNECTION_STRING,
});

export async function connectCleanedDB() {
  try {
    await cleanedClient.connect();
    console.log('Connected to Looker Studio cleaned database');
  } catch (error) {
    console.error('Failed to connect to Looker Studio DB:', error);
    throw error;
  }
}
