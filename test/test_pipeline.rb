#!/usr/bin/env ruby
#
# Integration test for the full Polar Hub pipeline.
# Requires a running hub in test mode + InfluxDB.
#
# Usage:
#   npm run server:test          # terminal 1
#   ruby test/test_pipeline.rb   # terminal 2
#

require 'net/http'
require 'json'
require 'influxdb'

HUB_URL     = ENV.fetch('HUB_URL',     'http://localhost:3002')  # must match testPort in config.json
INFLUX_HOST = ENV.fetch('INFLUX_HOST',  '192.168.0.10')
INFLUX_PORT = ENV.fetch('INFLUX_PORT',  '8086').to_i
INFLUX_DB   = ENV.fetch('INFLUX_DB',    'home_test')
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

def count_measurement(influx, measurement, device)
  q = "SELECT count(rr_interval) FROM #{measurement} WHERE \"device\" = '#{device}'"
  r = influx.query(q)
  return 0 if r.empty? || r.first['values'].nil?
  r.first['values'].first['count']
rescue InfluxDB::QueryError
  0
end

# Base timestamp: 1 hour ago
BASE_TS = (Time.now.to_f * 1000).to_i - 3_600_000

# 60 RR values for real-time (800..859 ms)
RR_NORMAL = (0...60).map { |i| 800.0 + i }

# Separate window for heartRate tests
HR_TEST_START = BASE_TS + 300_000

# ── Test 1: Clean slate ──────────────────────────────────────────────────────

test("Clean slate") do
  influx.create_database(INFLUX_DB) rescue nil
  influx.query("DELETE FROM polar_raw WHERE \"device\" = '#{TEST_DEVICE}'") rescue nil
  influx.query("DELETE FROM polar_realtime WHERE \"device\" = '#{TEST_DEVICE}'") rescue nil
  influx.query("DELETE FROM polar_hrv_summary WHERE \"device\" = '#{TEST_DEVICE}'") rescue nil
  sleep 0.5

  c = count_measurement(influx, 'polar_raw', TEST_DEVICE)
  assert("No test data in polar_raw", c == 0, "Found #{c} points")
end

# ── Test 2: Real-time raw write ──────────────────────────────────────────────

test("Real-time: raw write — POST 60 beats to /beats") do
  60.times do |i|
    post_json('/beats', {
      source:       TEST_SOURCE,
      device:       TEST_DEVICE,
      timestamp:    BASE_TS + (i * 1000),
      heartRate:    75,
      rrIntervals:  [RR_NORMAL[i]]
    })
  end
  sleep 1

  c = count_measurement(influx, 'polar_raw', TEST_DEVICE)
  assert("60 points in polar_raw", c == 60, "Found #{c}")

  # Verify path='realtime'
  r = influx.query(
    "SELECT path FROM polar_raw " \
    "WHERE \"device\" = '#{TEST_DEVICE}' AND path = 'realtime' LIMIT 5"
  )
  rt_count = r.empty? || r.first['values'].nil? ? 0 : r.first['values'].length
  assert("Beats have path='realtime'", rt_count > 0, "Found #{rt_count}")
end

# ── Test 3: Real-time polar_realtime HRV ─────────────────────────────────────

test("Real-time: polar_realtime HRV") do
  r = influx.query(
    "SELECT rmssd FROM polar_realtime " \
    "WHERE \"device\" = '#{TEST_DEVICE}' AND rmssd > 0 LIMIT 5"
  )
  has_rmssd = !r.empty? && !r.first['values'].nil? && r.first['values'].length > 0
  assert("polar_realtime has entries with rmssd > 0", has_rmssd)
end

# ── Test 4: Batch dedup — full overlap ───────────────────────────────────────

test("Batch dedup: full overlap — same 60 beats") do
  beats = 60.times.map do |i|
    {
      timestamp:    BASE_TS + (i * 1000),
      heartRate:    75,
      rrIntervals:  [RR_NORMAL[i]]
    }
  end

  resp = post_json('/beats/batch', { source: TEST_SOURCE, device: TEST_DEVICE, beats: beats })

  assert("new == 0",         resp['new']        == 0,  "new=#{resp['new']}")
  assert("duplicates == 60", resp['duplicates'] == 60, "duplicates=#{resp['duplicates']}")

  c = count_measurement(influx, 'polar_raw', TEST_DEVICE)
  assert("Still 60 points (no duplicates)", c == 60, "Found #{c}")
end

# ── Test 5: Batch dedup — gap fill ───────────────────────────────────────────

test("Batch dedup: gap fill — skip beat #30, batch fills exactly 1") do
  influx.query("DELETE FROM polar_raw WHERE \"device\" = '#{TEST_DEVICE}'")
  sleep 0.5

  # Re-POST 59 beats (skip index 30)
  60.times do |i|
    next if i == 30
    post_json('/beats', {
      source:       TEST_SOURCE,
      device:       TEST_DEVICE,
      timestamp:    BASE_TS + (i * 1000),
      heartRate:    75,
      rrIntervals:  [RR_NORMAL[i]]
    })
  end
  sleep 1

  c = count_measurement(influx, 'polar_raw', TEST_DEVICE)
  assert("59 points after skipping 1 beat", c == 59, "Found #{c}")

  # Batch all 60 — should fill exactly the 1 gap
  beats = 60.times.map do |i|
    {
      timestamp:    BASE_TS + (i * 1000),
      heartRate:    75,
      rrIntervals:  [RR_NORMAL[i]]
    }
  end

  resp = post_json('/beats/batch', { source: TEST_SOURCE, device: TEST_DEVICE, beats: beats })

  assert("new >= 1",          resp['new']        >= 1,  "new=#{resp['new']}")
  assert("duplicates >= 58",  resp['duplicates'] >= 58, "duplicates=#{resp['duplicates']}")

  sleep 1
  c = count_measurement(influx, 'polar_raw', TEST_DEVICE)
  assert("60 total points after gap fill", c == 60, "Found #{c}")
end

# ── Test 6: Batch — heartRate optional ───────────────────────────────────────

test("Batch: heartRate optional — rrIntervals without heartRate") do
  beats = 5.times.map do |i|
    {
      timestamp:    HR_TEST_START + (i * 1000),
      rrIntervals:  [820.0 + i]
      # no heartRate key at all
    }
  end

  resp = post_json('/beats/batch', { source: TEST_SOURCE, device: TEST_DEVICE, beats: beats })
  assert("5 beats accepted", resp['new'] == 5, "new=#{resp['new']}")

  sleep 1
  r = influx.query(
    "SELECT rr_interval, heart_rate FROM polar_raw " \
    "WHERE \"device\" = '#{TEST_DEVICE}' " \
    "AND time >= #{HR_TEST_START}ms AND time <= #{HR_TEST_START + 5000}ms"
  )
  if r.empty? || r.first['values'].nil?
    assert("Points written in range", false, "No points found")
  else
    assert("Points written in range", r.first['values'].length == 5,
           "Expected 5, found #{r.first['values'].length}")
    all_null = r.first['values'].all? { |v| v['heart_rate'].nil? }
    assert("heart_rate absent (null) on all points", all_null,
           "Some points have heart_rate: #{r.first['values'].map { |v| v['heart_rate'] }}")
  end
end

# ── Test 7: Batch — heartRate=0 as null ──────────────────────────────────────

test("Batch: heartRate=0 treated as null") do
  hr0_start = HR_TEST_START + 10_000
  beats = 5.times.map do |i|
    {
      timestamp:    hr0_start + (i * 1000),
      heartRate:    0,
      rrIntervals:  [830.0 + i]
    }
  end

  resp = post_json('/beats/batch', { source: TEST_SOURCE, device: TEST_DEVICE, beats: beats })
  assert("5 beats accepted", resp['new'] == 5, "new=#{resp['new']}")

  sleep 1
  r = influx.query(
    "SELECT rr_interval, heart_rate FROM polar_raw " \
    "WHERE \"device\" = '#{TEST_DEVICE}' " \
    "AND time >= #{hr0_start}ms AND time <= #{hr0_start + 5000}ms"
  )
  if r.empty? || r.first['values'].nil?
    assert("Points written in range", false, "No points found")
  else
    assert("Points written in range", r.first['values'].length == 5,
           "Expected 5, found #{r.first['values'].length}")
    all_null = r.first['values'].all? { |v| v['heart_rate'].nil? }
    assert("heart_rate absent when sent as 0", all_null,
           "Some points have heart_rate: #{r.first['values'].map { |v| v['heart_rate'] }}")
  end
end

# ── Test 8: Post-processor — artifact detection ─────────────────────────────

test("Post-processor: artifact detection") do
  # Build 200 beats: mostly ~800ms with known artifacts
  artifact_start = BASE_TS + 600_000
  rr_values = Array.new(200, 800.0)

  # Ectopic pair at positions 50-51: short then long (reversed compensatory)
  rr_values[50] = 500.0
  rr_values[51] = 1100.0

  # Missed beat at position 100: ~2x normal
  rr_values[100] = 1600.0

  beats = []
  ts = artifact_start
  rr_values.each do |rr|
    beats << { timestamp: ts, heartRate: 75, rrIntervals: [rr] }
    ts += rr.to_i
  end

  resp = post_json('/beats/batch', { source: TEST_SOURCE, device: TEST_DEVICE, beats: beats })
  assert("200 beats accepted", resp['received'] == 200, "received=#{resp['received']}")

  # Poll for rr_clean (post-processor runs every 60s, processes beats >120s old)
  artifact_end = artifact_start + rr_values.sum.to_i
  rr_clean_count = 0
  max_attempts = 18  # 18 * 5s = 90s
  attempt = 0

  puts "  Waiting for post-processor (polling every 5s, up to 90s)..."
  loop do
    attempt += 1
    r = influx.query(
      "SELECT count(rr_clean) FROM polar_raw " \
      "WHERE \"device\" = '#{TEST_DEVICE}' " \
      "AND time >= #{artifact_start}ms AND time <= #{artifact_end}ms " \
      "AND rr_clean > 0"
    )
    rr_clean_count = if r.empty? || r.first['values'].nil?
                       0
                     else
                       r.first['values'].first['count']
                     end

    break if rr_clean_count > 0 || attempt >= max_attempts
    sleep 5
  end

  assert("rr_clean values written (#{rr_clean_count} found)", rr_clean_count > 0,
         "Post-processor may not have run yet (waited #{attempt * 5}s)")

  if rr_clean_count > 0
    # Check for artifact_type on known artifact positions
    r = influx.query(
      "SELECT artifact_type FROM polar_raw " \
      "WHERE \"device\" = '#{TEST_DEVICE}' " \
      "AND time >= #{artifact_start}ms AND time <= #{artifact_end}ms " \
      "AND artifact_type <> 'none' " \
      "LIMIT 20"
    )
    artifact_count = r.empty? || r.first['values'].nil? ? 0 : r.first['values'].length
    assert("Artifacts detected (#{artifact_count} found)", artifact_count > 0)

    if artifact_count > 0
      types = r.first['values'].map { |v| v['artifact_type'] }.uniq
      assert("Artifact types include ectopic or missed", types.any? { |t| %w[ectopic missed].include?(t) },
             "Types found: #{types}")
    end
  end
end

# ── Test 9: Post-processor — hr_clean ────────────────────────────────────────

test("Post-processor: hr_clean") do
  artifact_start = BASE_TS + 600_000
  artifact_end = artifact_start + 200 * 800  # approximate

  r = influx.query(
    "SELECT hr_clean FROM polar_raw " \
    "WHERE \"device\" = '#{TEST_DEVICE}' " \
    "AND time >= #{artifact_start}ms AND time <= #{artifact_end}ms " \
    "AND hr_clean > 0 LIMIT 5"
  )
  hr_count = r.empty? || r.first['values'].nil? ? 0 : r.first['values'].length
  assert("hr_clean > 0 on processed beats", hr_count > 0, "Found #{hr_count}")
end

# ── Test 10: 5-min summaries ─────────────────────────────────────────────────

test("5-min summaries") do
  r = influx.query(
    "SELECT rmssd, sample_count FROM polar_hrv_summary " \
    "WHERE \"device\" = '#{TEST_DEVICE}'"
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

# ── Test 11: Cleanup ─────────────────────────────────────────────────────────

test("Cleanup") do
  influx.query("DELETE FROM polar_raw WHERE \"device\" = '#{TEST_DEVICE}'")
  influx.query("DELETE FROM polar_realtime WHERE \"device\" = '#{TEST_DEVICE}'")
  influx.query("DELETE FROM polar_hrv_summary WHERE \"device\" = '#{TEST_DEVICE}'")
  sleep 0.5

  c = count_measurement(influx, 'polar_raw', TEST_DEVICE)
  assert("All test data cleaned up", c == 0, "Found #{c} remaining points")
end

# ── Results ──────────────────────────────────────────────────────────────────

puts "\n#{'=' * 60}"
if $failures == 0
  puts "ALL #{$test_num} TESTS PASSED"
else
  puts "#{$failures} FAILURE(S) across #{$test_num} tests"
end
puts '=' * 60

exit($failures == 0 ? 0 : 1)
