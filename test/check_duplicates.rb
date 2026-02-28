#!/usr/bin/env ruby
#
# Duplicate detection for polar_raw.
# Checks for batch beats that have a realtime neighbor within 500ms,
# indicating the dedup failed and both paths wrote the same heartbeat.
#
# Usage:
#   ruby test/check_duplicates.rb              # check last 30 minutes
#   ruby test/check_duplicates.rb 60           # check last 60 minutes
#   INFLUX_HOST=localhost ruby test/check_duplicates.rb
#
# Exit codes:
#   0 = no duplicates found
#   1 = duplicates detected
#
# Cron example (every 30 min):
#   */30 * * * * ruby /path/to/check_duplicates.rb >> /path/to/logs/dedup-check.log 2>&1
#

require 'influxdb'
require 'time'

INFLUX_HOST = ENV.fetch('INFLUX_HOST', '192.168.0.10')
INFLUX_PORT = ENV.fetch('INFLUX_PORT', '8086').to_i
INFLUX_DB   = ENV.fetch('INFLUX_DB',   'home')
WINDOW_MIN  = (ARGV[0] || 30).to_i
PROXIMITY_MS = 500

influx = InfluxDB::Client.new(
  host: INFLUX_HOST,
  port: INFLUX_PORT,
  database: INFLUX_DB,
  time_precision: 'ms'
)

now = (Time.now.to_f * 1000).to_i
window_start = now - (WINDOW_MIN * 60_000)

# Query batch and realtime beats separately
batch = influx.query(
  "SELECT time, rr_interval FROM polar_raw " \
  "WHERE path = 'batch' AND time >= #{window_start}ms ORDER BY time ASC"
) rescue []

realtime = influx.query(
  "SELECT time, rr_interval FROM polar_raw " \
  "WHERE path = 'realtime' AND time >= #{window_start}ms ORDER BY time ASC"
) rescue []

batch_beats = batch.empty? || batch.first['values'].nil? ? [] : batch.first['values']
rt_beats = realtime.empty? || realtime.first['values'].nil? ? [] : realtime.first['values']

# Parse timestamps
batch_beats = batch_beats.map { |b| { time: Time.parse(b['time']).to_f * 1000, rr: b['rr_interval'] } }
rt_beats = rt_beats.map { |b| { time: Time.parse(b['time']).to_f * 1000, rr: b['rr_interval'] } }

if batch_beats.empty?
  puts "[#{Time.now.strftime('%Y-%m-%d %H:%M')}] OK — no batch beats in last #{WINDOW_MIN}min (#{rt_beats.length} realtime)"
  exit 0
end

# For each batch beat, find nearest realtime beat
duplicates = 0
rt_idx = 0

batch_beats.each do |b|
  # Advance rt_idx to the neighborhood
  while rt_idx < rt_beats.length - 1 && rt_beats[rt_idx + 1][:time] < b[:time] - PROXIMITY_MS
    rt_idx += 1
  end

  # Check nearby realtime beats
  found = false
  i = [rt_idx - 1, 0].max
  while i < rt_beats.length && rt_beats[i][:time] <= b[:time] + PROXIMITY_MS
    if (rt_beats[i][:time] - b[:time]).abs <= PROXIMITY_MS
      found = true
      break
    end
    i += 1
  end

  duplicates += 1 if found
end

gap_fills = batch_beats.length - duplicates
ts = Time.now.strftime('%Y-%m-%d %H:%M')

if duplicates > 0
  puts "[#{ts}] DUPLICATES — #{duplicates} batch beats have realtime neighbor within #{PROXIMITY_MS}ms " \
       "(#{gap_fills} legitimate gap fills, #{rt_beats.length} realtime) in last #{WINDOW_MIN}min"
  exit 1
else
  puts "[#{ts}] OK — #{batch_beats.length} batch beats, all are gap fills " \
       "(#{rt_beats.length} realtime) in last #{WINDOW_MIN}min"
  exit 0
end
