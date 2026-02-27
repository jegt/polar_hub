#!/usr/bin/env ruby
#
# Integration test for batch dedup & HRV recomputation.
# Requires a running hub + InfluxDB.
#
# Usage:
#   ruby hub/test/test_batch_dedup.rb
#   HUB_URL=http://host:3000 INFLUX_HOST=localhost ruby hub/test/test_batch_dedup.rb
#

require 'net/http'
require 'json'
require 'influxdb'

HUB_URL     = ENV.fetch('HUB_URL',     'http://localhost:3000')
INFLUX_HOST = ENV.fetch('INFLUX_HOST',  'localhost')
INFLUX_PORT = ENV.fetch('INFLUX_PORT',  '8086').to_i
INFLUX_DB   = ENV.fetch('INFLUX_DB',    'polar_hub')
TEST_DEVICE = 'TEST:DE:VI:CE:00:01'
TEST_SOURCE = 'test-script'

influx = InfluxDB::Client.new(
  host: INFLUX_HOST,
  port: INFLUX_PORT,
  database: INFLUX_DB,
  time_precision: 'ms'
)

$failures = 0
$test_num = 0

# ── Helpers ──────────────────────────────────────────────────────────────────

def post_json(path, body)
  uri = URI("#{HUB_URL}#{path}")
  req = Net::HTTP::Post.new(uri, 'Content-Type' => 'application/json')
  req.body = body.to_json
  res = Net::HTTP.start(uri.hostname, uri.port) { |http| http.request(req) }
  JSON.parse(res.body)
end

def assert(desc, condition, detail = nil)
  if condition
    puts "  \u2713 #{desc}"
  else
    puts "  \u2717 #{desc}"
    puts "    #{detail}" if detail
    $failures += 1
  end
end

def test(name)
  $test_num += 1
  puts "\n=== Test #{$test_num}: #{name} ==="
  yield
end

def count_raw(influx, device, extra_where = '')
  q = "SELECT count(rr_interval) FROM polar_hrv_raw WHERE \"device\" = '#{device}'#{extra_where}"
  r = influx.query(q)
  return 0 if r.empty? || r.first['values'].nil?
  r.first['values'].first['count']
end

# Base timestamp: 1 hour ago (avoids colliding with real data)
BASE_TS = (Time.now.to_f * 1000).to_i - 3_600_000

# 60 unique RR values for real-time window (800..859 ms)
RR_RT = (0...60).map { |i| 800.0 + i }
# 60 unique RR values for gap-fill window (900..959 ms)
RR_GAP = (0...60).map { |i| 900.0 + i }
# 30 new RR values for the mixed-batch extension (960..989 ms)
RR_MIX_NEW = (0...30).map { |i| 960.0 + i }

# Window 1 (real-time): BASE_TS .. BASE_TS + 59 000
# Window 2 (gap fill):  BASE_TS + 120 000 .. BASE_TS + 179 000
# Window 3 (mixed new): BASE_TS + 180 000 .. BASE_TS + 209 000
W1_START = BASE_TS
W2_START = BASE_TS + 120_000
W3_START = BASE_TS + 180_000

# ── Test 1: Clean slate ──────────────────────────────────────────────────────

test("Clean slate") do
  influx.query("DELETE FROM polar_hrv_raw WHERE \"device\" = '#{TEST_DEVICE}'")
  # Delete summaries in our test time range (no device tag on summaries)
  range_start = BASE_TS - 600_000
  range_end   = BASE_TS + 600_000
  influx.query("DELETE FROM polar_hrv WHERE time >= #{range_start}ms AND time <= #{range_end}ms")
  sleep 0.5

  c = count_raw(influx, TEST_DEVICE)
  assert("No test data in polar_hrv_raw", c == 0, "Found #{c} points")
end

# ── Test 2: Real-time simulation ─────────────────────────────────────────────

test("Real-time simulation — POST 60 beats to /beats") do
  60.times do |i|
    post_json('/beats', {
      source:       TEST_SOURCE,
      device:       TEST_DEVICE,
      timestamp:    W1_START + (i * 1000),
      heartRate:    75,
      rrIntervals:  [RR_RT[i]]
    })
  end
  sleep 1 # let InfluxDB flush

  c = count_raw(influx, TEST_DEVICE)
  assert("60 points in InfluxDB", c == 60, "Found #{c}")

  # Check rmssd populated on some points (warmup skips first 10)
  r = influx.query("SELECT rmssd FROM polar_hrv_raw WHERE \"device\" = '#{TEST_DEVICE}' AND rmssd > 0 LIMIT 5")
  has_rmssd = !r.empty? && !r.first['values'].nil? && r.first['values'].length > 0
  assert("RMSSD values populated on some beats", has_rmssd)
end

# ── Test 3: Overlapping batch — same beats (expect 0 new with gap dedup) ─────

test("Overlapping batch — same 60 beats, no gaps to fill") do
  beats = 60.times.map do |i|
    {
      timestamp:    W1_START + (i * 1000),
      heartRate:    75,
      rrIntervals:  [RR_RT[i]]
    }
  end

  resp = post_json('/beats/batch', { source: TEST_SOURCE, device: TEST_DEVICE, beats: beats })

  assert("received == 60",    resp['received']   == 60,  "received=#{resp['received']}")
  assert("new == 0",          resp['new']        == 0,   "new=#{resp['new']}")
  assert("duplicates == 60",  resp['duplicates'] == 60,  "duplicates=#{resp['duplicates']}")

  c = count_raw(influx, TEST_DEVICE)
  assert("Still 60 points (no duplicates created)", c == 60, "Found #{c}")
end

# ── Test 3b: Single missing beat — gap fill exactly 1 beat ───────────────────

test("Single missing beat — skip beat 30, batch fills exactly 1") do
  # Delete all test data and re-POST 59 beats (skipping index 30)
  influx.query("DELETE FROM polar_hrv_raw WHERE \"device\" = '#{TEST_DEVICE}'")
  sleep 0.5

  60.times do |i|
    next if i == 30  # skip this beat to create a gap
    post_json('/beats', {
      source:       TEST_SOURCE,
      device:       TEST_DEVICE,
      timestamp:    W1_START + (i * 1000),
      heartRate:    75,
      rrIntervals:  [RR_RT[i]]
    })
  end
  sleep 1

  c = count_raw(influx, TEST_DEVICE)
  assert("59 points after skipping 1 beat", c == 59, "Found #{c}")

  # Now POST full 60-beat batch — should fill exactly the 1 gap
  beats = 60.times.map do |i|
    {
      timestamp:    W1_START + (i * 1000),
      heartRate:    75,
      rrIntervals:  [RR_RT[i]]
    }
  end

  resp = post_json('/beats/batch', { source: TEST_SOURCE, device: TEST_DEVICE, beats: beats })

  assert("received == 60",    resp['received']   == 60,  "received=#{resp['received']}")
  assert("new == 1",          resp['new']        == 1,   "new=#{resp['new']}")
  assert("duplicates == 59",  resp['duplicates'] == 59,  "duplicates=#{resp['duplicates']}")

  sleep 1
  c = count_raw(influx, TEST_DEVICE)
  assert("60 total points after gap fill", c == 60, "Found #{c}")
end

# ── Test 3c: Path field verification ────────────────────────────────────────

test("Path field — realtime vs batch") do
  # Real-time beats should have path='realtime'
  r = influx.query(
    "SELECT path FROM polar_hrv_raw " \
    "WHERE \"device\" = '#{TEST_DEVICE}' AND path = 'realtime' LIMIT 5"
  )
  rt_count = r.empty? || r.first['values'].nil? ? 0 : r.first['values'].length
  assert("Real-time beats have path='realtime'", rt_count > 0, "Found #{rt_count}")

  # The gap-filled beat should have path='batch'
  r = influx.query(
    "SELECT path FROM polar_hrv_raw " \
    "WHERE \"device\" = '#{TEST_DEVICE}' AND path = 'batch' LIMIT 5"
  )
  batch_count = r.empty? || r.first['values'].nil? ? 0 : r.first['values'].length
  assert("Batch-inserted beat has path='batch'", batch_count > 0, "Found #{batch_count}")
end

# ── Test 4: Gap-filling batch — new 60 s window (expect all new) ─────────────

test("Gap-filling batch — 60 new beats in separate window") do
  beats = 60.times.map do |i|
    {
      timestamp:    W2_START + (i * 1000),
      heartRate:    72,
      rrIntervals:  [RR_GAP[i]]
    }
  end

  resp = post_json('/beats/batch', { source: TEST_SOURCE, device: TEST_DEVICE, beats: beats })

  assert("received == 60",    resp['received']   == 60,  "received=#{resp['received']}")
  assert("new == 60",         resp['new']        == 60,  "new=#{resp['new']}")
  assert("duplicates == 0",   resp['duplicates'] == 0,   "duplicates=#{resp['duplicates']}")

  sleep 1
  c = count_raw(influx, TEST_DEVICE)
  assert("120 total points", c == 120, "Found #{c}")
end

# ── Test 5: Mixed batch — 30 overlap + 30 new ────────────────────────────────

test("Mixed batch — 30 overlapping + 30 new beats") do
  beats = []
  # 30 beats overlapping with gap-fill window (same RR, offset timestamp)
  30.times do |i|
    beats << {
      timestamp:    W2_START + (i * 1000) + 500,  # +0.5 s offset
      heartRate:    72,
      rrIntervals:  [RR_GAP[i]]
    }
  end
  # 30 genuinely new beats
  30.times do |i|
    beats << {
      timestamp:    W3_START + (i * 1000),
      heartRate:    70,
      rrIntervals:  [RR_MIX_NEW[i]]
    }
  end

  resp = post_json('/beats/batch', { source: TEST_SOURCE, device: TEST_DEVICE, beats: beats })

  assert("received == 60",    resp['received']   == 60,  "received=#{resp['received']}")
  assert("new == 30",         resp['new']        == 30,  "new=#{resp['new']}")
  assert("duplicates == 30",  resp['duplicates'] == 30,  "duplicates=#{resp['duplicates']}")

  sleep 1
  c = count_raw(influx, TEST_DEVICE)
  assert("150 total points", c == 150, "Found #{c}")
end

# ── Test 6: Idempotent re-upload of gap-fill batch ───────────────────────────

test("Idempotent re-upload — gap-fill batch again") do
  beats = 60.times.map do |i|
    {
      timestamp:    W2_START + (i * 1000),
      heartRate:    72,
      rrIntervals:  [RR_GAP[i]]
    }
  end

  resp = post_json('/beats/batch', { source: TEST_SOURCE, device: TEST_DEVICE, beats: beats })

  assert("new == 0",          resp['new']        == 0,   "new=#{resp['new']}")
  assert("duplicates == 60",  resp['duplicates'] == 60,  "duplicates=#{resp['duplicates']}")

  sleep 1
  c = count_raw(influx, TEST_DEVICE)
  assert("Still 150 points", c == 150, "Found #{c}")
end

# ── Test 7: RMSSD recomputation ──────────────────────────────────────────────

test("RMSSD recomputation — batch window beats have rmssd") do
  # Beats in the gap-fill window should have rmssd after batch recomputation
  w2_end = W2_START + 59_000
  r = influx.query(
    "SELECT rmssd FROM polar_hrv_raw " \
    "WHERE \"device\" = '#{TEST_DEVICE}' " \
    "AND time >= #{W2_START}ms AND time <= #{w2_end}ms " \
    "AND rmssd > 0"
  )
  rmssd_count = r.empty? || r.first['values'].nil? ? 0 : r.first['values'].length
  # Need at least 2 RR values for RMSSD, so not all 60 will have it,
  # but most should (59 out of 60, minus any edge cases)
  assert("Batch window beats have rmssd (#{rmssd_count}/60)", rmssd_count >= 50,
         "Only #{rmssd_count} beats have rmssd")

  # Check tail beats (after the gap-fill window) also got updated
  # The mixed-batch beats start at W3_START; check if they have rmssd
  w3_end = W3_START + 29_000
  r = influx.query(
    "SELECT rmssd FROM polar_hrv_raw " \
    "WHERE \"device\" = '#{TEST_DEVICE}' " \
    "AND time >= #{W3_START}ms AND time <= #{w3_end}ms " \
    "AND rmssd > 0"
  )
  tail_count = r.empty? || r.first['values'].nil? ? 0 : r.first['values'].length
  assert("Tail beats (post-batch) have rmssd (#{tail_count}/30)", tail_count >= 20,
         "Only #{tail_count} tail beats have rmssd")
end

# ── Test 8: 5-min HRV summary recomputation ─────────────────────────────────

test("5-min HRV summary recomputation") do
  # Summaries are written at window_end timestamps with 5-min (300 000 ms) intervals
  range_start = BASE_TS - 300_000
  range_end   = BASE_TS + 600_000
  r = influx.query(
    "SELECT rmssd, sample_count FROM polar_hrv " \
    "WHERE time >= #{range_start}ms AND time <= #{range_end}ms"
  )
  summary_count = r.empty? || r.first['values'].nil? ? 0 : r.first['values'].length
  assert("At least 1 summary written", summary_count >= 1,
         "Found #{summary_count} summaries")

  if summary_count > 0
    sample = r.first['values'].first
    has_rmssd = sample['rmssd'] && sample['rmssd'] > 0
    has_count = sample['sample_count'] && sample['sample_count'] >= 10
    assert("Summary has rmssd > 0", has_rmssd, "rmssd=#{sample['rmssd']}")
    assert("Summary has sample_count >= 10", has_count, "sample_count=#{sample['sample_count']}")
  end
end

# ── Cleanup ──────────────────────────────────────────────────────────────────

puts "\n=== Cleanup ==="
influx.query("DELETE FROM polar_hrv_raw WHERE \"device\" = '#{TEST_DEVICE}'")
range_start = BASE_TS - 600_000
range_end   = BASE_TS + 600_000
influx.query("DELETE FROM polar_hrv WHERE time >= #{range_start}ms AND time <= #{range_end}ms")
puts "  Test data cleaned up."

# ── Results ──────────────────────────────────────────────────────────────────

puts "\n#{'=' * 60}"
if $failures == 0
  puts "ALL #{$test_num} TESTS PASSED"
else
  puts "#{$failures} FAILURE(S) across #{$test_num} tests"
end
puts '=' * 60

exit($failures == 0 ? 0 : 1)
