#!/usr/bin/env ruby
#
# Duplicate detection for polar_raw.
# Checks for batch beats that have a realtime neighbor within 500ms,
# indicating dedup failed and both paths wrote the same heartbeat.
# Reports separately for batch_type=buffered_realtime and batch_type=recording.
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

# Query realtime beats
realtime = influx.query(
  "SELECT time, rr_interval FROM polar_raw " \
  "WHERE path = 'realtime' AND time >= #{window_start}ms ORDER BY time ASC"
) rescue []

rt_beats = realtime.empty? || realtime.first['values'].nil? ? [] : realtime.first['values']
rt_beats = rt_beats.map { |b| { time: Time.parse(b['time']).to_f * 1000, rr: b['rr_interval'] } }

def load_batch_type(influx, batch_type, window_start)
  rows = influx.query(
    "SELECT time, rr_interval FROM polar_raw " \
    "WHERE path = 'batch' AND batch_type = '#{batch_type}' " \
    "AND time >= #{window_start}ms ORDER BY time ASC"
  ) rescue []
  values = rows.empty? || rows.first['values'].nil? ? [] : rows.first['values']
  values.map { |b| { time: Time.parse(b['time']).to_f * 1000, rr: b['rr_interval'] } }
end

def count_duplicates(batch_beats, rt_beats, proximity_ms)
  return 0 if batch_beats.empty?

  duplicates = 0
  rt_idx = 0

  batch_beats.each do |b|
    while rt_idx < rt_beats.length - 1 && rt_beats[rt_idx + 1][:time] < b[:time] - proximity_ms
      rt_idx += 1
    end

    found = false
    i = [rt_idx - 1, 0].max
    while i < rt_beats.length && rt_beats[i][:time] <= b[:time] + proximity_ms
      if (rt_beats[i][:time] - b[:time]).abs <= proximity_ms
        found = true
        break
      end
      i += 1
    end

    duplicates += 1 if found
  end

  duplicates
end
ts = Time.now.strftime('%Y-%m-%d %H:%M')
buffered = load_batch_type(influx, 'buffered_realtime', window_start)
recording = load_batch_type(influx, 'recording', window_start)

buf_dup = count_duplicates(buffered, rt_beats, PROXIMITY_MS)
rec_dup = count_duplicates(recording, rt_beats, PROXIMITY_MS)
total_dup = buf_dup + rec_dup

if buffered.empty? && recording.empty?
  puts "[#{ts}] OK — no batch beats in last #{WINDOW_MIN}min (#{rt_beats.length} realtime)"
  exit 0
end

buf_gap = buffered.length - buf_dup
rec_gap = recording.length - rec_dup

if total_dup > 0
  puts "[#{ts}] DUPLICATES — buffered=#{buf_dup}/#{buffered.length} (gap_fills=#{buf_gap}), " \
       "recording=#{rec_dup}/#{recording.length} (gap_fills=#{rec_gap}), realtime=#{rt_beats.length}, " \
       "window=#{WINDOW_MIN}min"
  exit 1
else
  puts "[#{ts}] OK — buffered=#{buffered.length} (all gap fills), recording=#{recording.length} (all gap fills), " \
       "realtime=#{rt_beats.length}, window=#{WINDOW_MIN}min"
  exit 0
end
