import { Client } from 'pg';
import dotenv from 'dotenv';

// Load environment variables
dotenv.config();

export const client = new Client({
  connectionString: process.env.PGVECTOR_CONNECTION_STRING,
});

export async function connectDB() {
  try {
    await client.connect();
    console.log('Connected to database');
  } catch (error) {
    console.error('Failed to connect to database:', error);
    throw error;
  }
}
