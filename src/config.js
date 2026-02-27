/**
 * Configuration loader for Polar Hub
 *
 * Reads config.json from the project root. Falls back to defaults
 * if the file is missing or individual keys are absent.
 */

import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const CONFIG_PATH = join(__dirname, '..', 'config.json');

const DEFAULTS = {
  influxHost: 'localhost',
  influxPort: 8086,
  influxDatabase: 'polar_hub',
  port: 3000,
  hrvSummaryIntervalMs: 300000
};

let userConfig = {};
try {
  userConfig = JSON.parse(readFileSync(CONFIG_PATH, 'utf-8'));
} catch (err) {
  if (err.code === 'ENOENT') {
    console.log('[config] config.json not found, using defaults');
  } else {
    console.error(`[config] Error reading config.json: ${err.message}`);
  }
}

const config = { ...DEFAULTS, ...userConfig };

export default config;
