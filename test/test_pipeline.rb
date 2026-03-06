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
TEST_SESSION_ID = 'test-session-001'

influx = InfluxDB::Client.new(
  host: INFLUX_HOST,
  port: INFLUX_PORT,
  database: INFLUX_DB,
  time_precision: 'ms'
)

$failures = 0
$test_num = 0
$upload_seq = 0

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

def next_upload_id(prefix)
  $upload_seq += 1
  "#{prefix}-#{$upload_seq}"
end

def poll_until(timeout_seconds: 90, interval_seconds: 5)
  deadline = Time.now + timeout_seconds
  last = nil
  loop do
    last = yield
    return [true, last] if last
    break if Time.now >= deadline
    sleep interval_seconds
  end
  [false, last]
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
  influx.query("DELETE FROM polar_quality_1m WHERE \"device\" = '#{TEST_DEVICE}'") rescue nil
  influx.query("DELETE FROM polar_upload_receipts WHERE \"device\" = '#{TEST_DEVICE}'") rescue nil
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
      sessionId:    TEST_SESSION_ID,
      beatSeq:      i + 1,
      timestamp:    BASE_TS + (i * 1000),
      heartRate:    75,
      rrInterval:   RR_NORMAL[i]
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
    "SELECT rmssd, hr, hr_instant FROM polar_realtime " \
    "WHERE \"device\" = '#{TEST_DEVICE}' AND rmssd > 0 LIMIT 5"
  )
  has_rmssd = !r.empty? && !r.first['values'].nil? && r.first['values'].length > 0
  assert("polar_realtime has entries with rmssd > 0", has_rmssd)
  if has_rmssd
    values = r.first['values']
    has_hr = values.any? { |v| !v['hr'].nil? && v['hr'] > 0 }
    has_hr_instant = values.any? { |v| !v['hr_instant'].nil? && v['hr_instant'] > 0 }
    assert("polar_realtime includes hr", has_hr)
    assert("polar_realtime includes hr_instant", has_hr_instant)
  end
end

# ── Test 3b: Real-time validation ────────────────────────────────────────────

test("Real-time: validation rejects missing rrInterval") do
  resp = post_json('/beats', {
    source: TEST_SOURCE,
    device: TEST_DEVICE,
    sessionId: TEST_SESSION_ID,
    beatSeq: 9999,
    timestamp: BASE_TS + 999_000
    # rrInterval omitted on purpose
  })

  assert("Request rejected", resp['ok'] == false, "ok=#{resp['ok']}")
  has_msg = resp['error'].is_a?(String) && resp['error'].include?('rrInterval')
  assert("Error mentions rrInterval", has_msg, "error=#{resp['error']}")
end

# ── Test 4: Batch dedup — full overlap ───────────────────────────────────────

test("Batch dedup: full overlap — same 60 beats") do
  beats = 60.times.map do |i|
    {
      sessionId:    TEST_SESSION_ID,
      beatSeq:      i + 1,
      timestamp:    BASE_TS + (i * 1000),
      heartRate:    75,
      rrInterval:   RR_NORMAL[i]
    }
  end

  resp = post_json('/beats/batch', {
    source: TEST_SOURCE,
    device: TEST_DEVICE,
    batchType: 'buffered_realtime',
    uploadId: next_upload_id('buf'),
    beats: beats
  })

  assert("new == 0",         resp['new']        == 0,  "new=#{resp['new']}")
  assert("duplicates == 60", resp['duplicates'] == 60, "duplicates=#{resp['duplicates']}")

  c = count_measurement(influx, 'polar_raw', TEST_DEVICE)
  assert("Still 60 points (no duplicates)", c == 60, "Found #{c}")
end

# ── Test 4b: Batch dedup — BLE-offset overlap ────────────────────────────────

test("Batch dedup: BLE-offset overlap — batch 400ms earlier than realtime") do
  # Realtime beats were sent at BASE_TS + i*1000 (test 2).
  # Batch beats arrive with recording timestamps ~400ms earlier
  # (BLE pipeline delay means realtime timestamps lag behind recording).
  # All 60 should be rejected as duplicates.
  beats = 60.times.map do |i|
    {
      sessionId:    TEST_SESSION_ID,
      beatSeq:      i + 1,
      timestamp:    BASE_TS + (i * 1000) - 400,
      rrInterval:   RR_NORMAL[i]
    }
  end

  resp = post_json('/beats/batch', {
    source: TEST_SOURCE,
    device: TEST_DEVICE,
    batchType: 'buffered_realtime',
    uploadId: next_upload_id('buf'),
    beats: beats
  })

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
      sessionId:    TEST_SESSION_ID,
      beatSeq:      i + 1,
      timestamp:    BASE_TS + (i * 1000),
      heartRate:    75,
      rrInterval:   RR_NORMAL[i]
    })
  end
  sleep 1

  c = count_measurement(influx, 'polar_raw', TEST_DEVICE)
  assert("59 points after skipping 1 beat", c == 59, "Found #{c}")

  # Batch all 60 — should fill exactly the 1 gap
  beats = 60.times.map do |i|
    {
      sessionId:    TEST_SESSION_ID,
      beatSeq:      i + 1,
      timestamp:    BASE_TS + (i * 1000),
      heartRate:    75,
      rrInterval:   RR_NORMAL[i]
    }
  end

  resp = post_json('/beats/batch', {
    source: TEST_SOURCE,
    device: TEST_DEVICE,
    batchType: 'buffered_realtime',
    uploadId: next_upload_id('buf'),
    beats: beats
  })

  assert("new == 1",          resp['new']        == 1,  "new=#{resp['new']}")
  assert("duplicates == 59",  resp['duplicates'] == 59, "duplicates=#{resp['duplicates']}")

  sleep 1
  c = count_measurement(influx, 'polar_raw', TEST_DEVICE)
  assert("60 total points after gap fill", c == 60, "Found #{c}")
end

# ── Test 5b: Recording dedup — gap fill ──────────────────────────────────────

test("Recording dedup: gap fill — skip beat #30, recording fills exactly 1") do
  influx.query("DELETE FROM polar_raw WHERE \"device\" = '#{TEST_DEVICE}'")
  sleep 0.5

  # Re-POST 59 realtime beats (skip index 30)
  60.times do |i|
    next if i == 30
    post_json('/beats', {
      source:       TEST_SOURCE,
      device:       TEST_DEVICE,
      sessionId:    'rt-gap-session',
      beatSeq:      i + 1,
      timestamp:    BASE_TS + (i * 1000),
      heartRate:    75,
      rrInterval:   RR_NORMAL[i]
    })
  end
  sleep 1

  c = count_measurement(influx, 'polar_raw', TEST_DEVICE)
  assert("59 points after skipping 1 beat", c == 59, "Found #{c}")

  recording_id = 'rec-gap-fill'
  beats = 60.times.map do |i|
    {
      recordingId:  recording_id,
      recordingSeq: i + 1,
      timestamp:    BASE_TS + (i * 1000),
      rrInterval:   RR_NORMAL[i]
    }
  end

  resp = post_json('/beats/batch', {
    source: TEST_SOURCE,
    device: TEST_DEVICE,
    batchType: 'recording',
    uploadId: next_upload_id('rec'),
    beats: beats
  })

  assert("new == 1",         resp['new'] == 1, "new=#{resp['new']}")
  assert("duplicates == 59", resp['duplicates'] == 59, "duplicates=#{resp['duplicates']}")

  sleep 1
  c = count_measurement(influx, 'polar_raw', TEST_DEVICE)
  assert("60 total points after recording gap fill", c == 60, "Found #{c}")
end

# ── Test 5c: uploadId replay idempotency ─────────────────────────────────────

test("Batch: uploadId replay returns replay=true and does not insert twice") do
  recording_id = 'rec-upload-replay'
  upload_id = next_upload_id('rec-replay')
  replay_start = BASE_TS + 900_000
  beats = 2.times.map do |i|
    {
      recordingId: recording_id,
      recordingSeq: i + 1,
      timestamp: replay_start + (i * 1000),
      rrInterval: 880.0 + i
    }
  end

  first = post_json('/beats/batch', {
    source: TEST_SOURCE,
    device: TEST_DEVICE,
    batchType: 'recording',
    uploadId: upload_id,
    beats: beats
  })
  assert("First upload replay=false", first['replay'] == false, "replay=#{first['replay']}")
  assert("First upload inserts 2", first['new'] == 2, "new=#{first['new']}")

  second = post_json('/beats/batch', {
    source: TEST_SOURCE,
    device: TEST_DEVICE,
    batchType: 'recording',
    uploadId: upload_id,
    beats: beats
  })
  assert("Second upload replay=true", second['replay'] == true, "replay=#{second['replay']}")

  r = influx.query(
    "SELECT count(rr_interval) FROM polar_raw " \
    "WHERE \"device\" = '#{TEST_DEVICE}' AND recording_id = '#{recording_id}'"
  )
  count = if r.empty? || r.first['values'].nil?
            0
          else
            r.first['values'].first['count']
          end
  assert("Still exactly 2 points for replay recording", count == 2, "count=#{count}")
end

# ── Test 6: Recording batch — heartRate optional ─────────────────────────────

test("Batch: heartRate optional — recording beats without heartRate") do
  recording_id = 'rec-heartrate-optional'
  beats = 5.times.map do |i|
    {
      recordingId:  recording_id,
      recordingSeq: i + 1,
      timestamp:    HR_TEST_START + (i * 1000),
      rrInterval:   820.0 + i
      # no heartRate key at all
    }
  end

  resp = post_json('/beats/batch', {
    source: TEST_SOURCE,
    device: TEST_DEVICE,
    batchType: 'recording',
    uploadId: next_upload_id('rec'),
    beats: beats
  })
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

# ── Test 7: Recording batch — heartRate=0 preserved ──────────────────────────

test("Batch: heartRate=0 stored as 0") do
  hr0_start = HR_TEST_START + 10_000
  recording_id = 'rec-heartrate-zero'
  beats = 5.times.map do |i|
    {
      recordingId:  recording_id,
      recordingSeq: i + 1,
      timestamp:    hr0_start + (i * 1000),
      heartRate:    0,
      rrInterval:   830.0 + i
    }
  end

  resp = post_json('/beats/batch', {
    source: TEST_SOURCE,
    device: TEST_DEVICE,
    batchType: 'recording',
    uploadId: next_upload_id('rec'),
    beats: beats
  })
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
    all_zero = r.first['values'].all? { |v| v['heart_rate'] == 0 }
    assert("heart_rate stored as 0 when sent as 0", all_zero,
           "heart_rate values: #{r.first['values'].map { |v| v['heart_rate'] }}")
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

  recording_id = 'rec-artifacts-200'
  beats = []
  ts = artifact_start
  rr_values.each_with_index do |rr, i|
    beats << {
      recordingId: recording_id,
      recordingSeq: i + 1,
      timestamp: ts,
      heartRate: 75,
      rrInterval: rr
    }
    ts += rr.to_i
  end

  resp = post_json('/beats/batch', {
    source: TEST_SOURCE,
    device: TEST_DEVICE,
    batchType: 'recording',
    uploadId: next_upload_id('rec'),
    beats: beats
  })
  assert("200 beats accepted", resp['received'] == 200, "received=#{resp['received']}")

  # Poll for rr_clean (post-processor runs every 60s, processes beats >120s old)
  artifact_end = artifact_start + rr_values.sum.to_i
  puts "  Waiting for post-processor (polling every 5s, up to 90s)..."
  ok, rr_clean_count = poll_until(timeout_seconds: 90, interval_seconds: 5) do
    r = influx.query(
      "SELECT count(rr_clean) FROM polar_raw " \
      "WHERE \"device\" = '#{TEST_DEVICE}' " \
      "AND time >= #{artifact_start}ms AND time <= #{artifact_end}ms " \
      "AND rr_clean > 0"
    )
    count = if r.empty? || r.first['values'].nil?
              0
            else
              r.first['values'].first['count']
            end
    count > 0 ? count : nil
  end

  rr_clean_count ||= 0
  assert("rr_clean values written (#{rr_clean_count} found)", ok,
         "Post-processor may not have run yet (waited 90s)")

  if ok
    # Check for artifact_type on known artifact positions.
    # Stronger than "any artifact": this should include ectopic and missed.
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
      assert("Artifact types include ectopic", types.include?('ectopic'),
             "Types found: #{types}")
      assert("Artifact types include missed", types.include?('missed'),
             "Types found: #{types}")
    end

    # No silent corrections: if rr_clean changes materially, artifact_type must not be 'none'
    r = influx.query(
      "SELECT rr_interval, rr_clean, artifact_type FROM polar_raw " \
      "WHERE \"device\" = '#{TEST_DEVICE}' " \
      "AND time >= #{artifact_start}ms AND time <= #{artifact_end}ms " \
      "AND rr_interval > 0 AND rr_clean > 0"
    )
    silent = 0
    unless r.empty? || r.first['values'].nil?
      r.first['values'].each do |v|
        type = v['artifact_type']
        next unless type.nil? || type == 'none'
        rr = v['rr_interval']
        rc = v['rr_clean']
        next if rr.nil? || rc.nil?
        silent += 1 if (rc - rr).abs > 0.5
      end
    end
    assert("No silent corrections (artifact_type='none' with changed rr_clean)", silent == 0,
           "silent_corrections=#{silent}")

    rt = influx.query(
      "SELECT hr, hr_instant FROM polar_realtime " \
      "WHERE \"device\" = '#{TEST_DEVICE}' " \
      "AND time >= #{artifact_start}ms AND time <= #{artifact_end}ms"
    )
    rt_values = (rt.empty? || rt.first['values'].nil?) ? [] : rt.first['values']
    assert("Post-processor rewrote polar_realtime for recording range", rt_values.length > 0,
           "realtime_points=#{rt_values.length}")
    if rt_values.length > 0
      has_rt_hr = rt_values.any? { |v| !v['hr'].nil? && v['hr'] > 0 }
      has_rt_inst = rt_values.any? { |v| !v['hr_instant'].nil? && v['hr_instant'] > 0 }
      assert("Rewritten polar_realtime has hr", has_rt_hr)
      assert("Rewritten polar_realtime has hr_instant", has_rt_inst)
    end
  end
end

# ── Test 9: Post-processor — boundary reprocess correctness ───────────────────

test("Post-processor: tail artifact is reclassified after later context arrives") do
  influx.query("DELETE FROM polar_raw WHERE \"device\" = '#{TEST_DEVICE}'")
  influx.query("DELETE FROM polar_hrv_summary WHERE \"device\" = '#{TEST_DEVICE}'")
  sleep 0.5

  tail_start = BASE_TS + 1_100_000
  recording_id = 'rec-tail-reprocess'

  rr_initial = Array.new(120, 800.0)
  rr_initial[118] = 1600.0 # second-to-last beat in first upload

  beats_1 = []
  ts = tail_start
  rr_initial.each_with_index do |rr, i|
    beats_1 << {
      recordingId: recording_id,
      recordingSeq: i + 1,
      timestamp: ts,
      rrInterval: rr
    }
    ts += rr.to_i
  end

  tail_artifact_ts = beats_1[118][:timestamp]

  resp1 = post_json('/beats/batch', {
    source: TEST_SOURCE,
    device: TEST_DEVICE,
    batchType: 'recording',
    uploadId: next_upload_id('rec-tail'),
    beats: beats_1
  })
  assert("First segment accepted", resp1['received'] == 120, "received=#{resp1['received']}")

  puts "  Waiting for post-processor first pass (polling every 5s, up to 90s)..."
  first_end = tail_start + rr_initial.sum.to_i
  ok_first, _ = poll_until(timeout_seconds: 90, interval_seconds: 5) do
    r = influx.query(
      "SELECT count(rr_clean) FROM polar_raw " \
      "WHERE \"device\" = '#{TEST_DEVICE}' " \
      "AND time >= #{tail_start}ms AND time <= #{first_end}ms " \
      "AND rr_clean > 0"
    )
    count = if r.empty? || r.first['values'].nil?
              0
            else
              r.first['values'].first['count']
            end
    count > 0 ? count : nil
  end
  assert("First segment processed", ok_first)

  before = influx.query(
    "SELECT artifact_type FROM polar_raw " \
    "WHERE \"device\" = '#{TEST_DEVICE}' AND time = #{tail_artifact_ts}ms LIMIT 1"
  )
  before_type = if before.empty? || before.first['values'].nil? || before.first['values'].empty?
                  nil
                else
                  before.first['values'].first['artifact_type']
                end
  assert("Tail beat initially unclassified", before_type.nil? || before_type == 'none',
         "artifact_type=#{before_type}")

  beats_2 = [
    {
      recordingId: recording_id,
      recordingSeq: 121,
      timestamp: ts + 2000,
      rrInterval: 800.0
    },
    {
      recordingId: recording_id,
      recordingSeq: 122,
      timestamp: ts + 2800,
      rrInterval: 800.0
    }
  ]

  resp2 = post_json('/beats/batch', {
    source: TEST_SOURCE,
    device: TEST_DEVICE,
    batchType: 'recording',
    uploadId: next_upload_id('rec-tail'),
    beats: beats_2
  })
  assert("Second segment inserted", resp2['new'] == 2, "new=#{resp2['new']}")

  puts "  Waiting for post-processor second pass (polling every 5s, up to 75s)..."
  ok_second, after_type = poll_until(timeout_seconds: 75, interval_seconds: 5) do
    after = influx.query(
      "SELECT artifact_type FROM polar_raw " \
      "WHERE \"device\" = '#{TEST_DEVICE}' AND time = #{tail_artifact_ts}ms LIMIT 1"
    )
    t = if after.empty? || after.first['values'].nil? || after.first['values'].empty?
          nil
        else
          after.first['values'].first['artifact_type']
        end
    t if t && t != 'none'
  end

  assert("Tail beat reclassified after context extension", ok_second,
         "artifact_type=#{after_type || 'none'}")
  if ok_second
    assert("Tail beat classified as missed", after_type == 'missed', "artifact_type=#{after_type}")
  end
end

# ── Test 10: Post-processor — out-of-order sequence handling ─────────────────

test("Post-processor: out-of-order timestamps do not create artifact bursts") do
  influx.query("DELETE FROM polar_raw WHERE \"device\" = '#{TEST_DEVICE}'")
  influx.query("DELETE FROM polar_hrv_summary WHERE \"device\" = '#{TEST_DEVICE}'")
  influx.query("DELETE FROM polar_quality_1m WHERE \"device\" = '#{TEST_DEVICE}'")
  sleep 0.5

  out_order_start = BASE_TS + 1_500_000
  recording_id = 'rec-out-of-order'
  rr_values = Array.new(140) { |i| 780.0 + ((i % 20) - 10) } # 770..789, smooth/clean

  beats = []
  ts = out_order_start
  rr_values.each_with_index do |rr, i|
    # Deliberately make part of the stream arrive with timestamps that jump backward.
    # Canonical analysis should still follow recording_seq.
    timestamp_jitter = (i >= 60 && i < 80) ? -15_000 : 0
    beats << {
      recordingId: recording_id,
      recordingSeq: i + 1,
      timestamp: ts + timestamp_jitter,
      rrInterval: rr
    }
    ts += rr.to_i
  end

  resp = post_json('/beats/batch', {
    source: TEST_SOURCE,
    device: TEST_DEVICE,
    batchType: 'recording',
    uploadId: next_upload_id('rec-ooo'),
    beats: beats
  })
  assert("Out-of-order batch accepted", resp['received'] == rr_values.length,
         "received=#{resp['received']}")

  puts "  Waiting for out-of-order batch post-processing (polling every 5s, up to 90s)..."
  ok, processed_count = poll_until(timeout_seconds: 90, interval_seconds: 5) do
    r = influx.query(
      "SELECT count(rr_clean) FROM polar_raw " \
      "WHERE \"device\" = '#{TEST_DEVICE}' AND recording_id = '#{recording_id}' AND rr_clean > 0"
    )
    count = if r.empty? || r.first['values'].nil?
              0
            else
              r.first['values'].first['count']
            end
    count >= 100 ? count : nil
  end

  processed_count ||= 0
  assert("Out-of-order batch processed (rr_clean present)", ok, "rr_clean_count=#{processed_count}")

  if ok
    r = influx.query(
      "SELECT count(artifact_type) FROM polar_raw " \
      "WHERE \"device\" = '#{TEST_DEVICE}' AND recording_id = '#{recording_id}' " \
      "AND artifact_type <> 'none'"
    )
    artifact_count = if r.empty? || r.first['values'].nil?
                       0
                     else
                       r.first['values'].first['count']
                     end
    assert("Out-of-order stream does not create artifact burst", artifact_count <= 5,
           "artifact_count=#{artifact_count}")

    min_ts = beats.map { |b| b[:timestamp] }.min
    max_ts = beats.map { |b| b[:timestamp] }.max + 120_000
    q = influx.query(
      "SELECT max(out_of_order_count), max(artifact_rate) FROM polar_quality_1m " \
      "WHERE \"device\" = '#{TEST_DEVICE}' AND time >= #{min_ts}ms AND time <= #{max_ts}ms"
    )
    max_out_of_order = if q.empty? || q.first['values'].nil? || q.first['values'].empty?
                         0
                       else
                         q.first['values'].first['max']
                       end
    max_artifact_rate = if q.empty? || q.first['values'].nil? || q.first['values'].empty?
                          0
                        else
                          q.first['values'].first['max_1']
                        end
    assert("Quality metrics report out-of-order beats", max_out_of_order && max_out_of_order > 0,
           "max_out_of_order_count=#{max_out_of_order}")
    assert("Artifact rate stays bounded for clean out-of-order stream",
           max_artifact_rate && max_artifact_rate < 0.15,
           "max_artifact_rate=#{max_artifact_rate}")
  end
end

# ── Test 11: Post-processor — extreme RR rails ──────────────────────────────

test("Post-processor: extreme RR values are corrected and bounded") do
  influx.query("DELETE FROM polar_raw WHERE \"device\" = '#{TEST_DEVICE}'")
  influx.query("DELETE FROM polar_hrv_summary WHERE \"device\" = '#{TEST_DEVICE}'")
  influx.query("DELETE FROM polar_quality_1m WHERE \"device\" = '#{TEST_DEVICE}'")
  sleep 0.5

  extreme_start = BASE_TS + 1_800_000
  recording_id = 'rec-extreme-rails'

  rr_values = Array.new(160, 800.0)
  rr_values[20] = 120.0   # too short
  rr_values[55] = 2600.0  # too long
  rr_values[90] = 250.0   # too short
  rr_values[130] = 3000.0 # too long

  beats = []
  ts = extreme_start
  rr_values.each_with_index do |rr, i|
    beats << {
      recordingId: recording_id,
      recordingSeq: i + 1,
      timestamp: ts,
      rrInterval: rr
    }
    ts += rr.to_i
  end

  resp = post_json('/beats/batch', {
    source: TEST_SOURCE,
    device: TEST_DEVICE,
    batchType: 'recording',
    uploadId: next_upload_id('rec-extreme'),
    beats: beats
  })
  assert("Extreme RR batch accepted", resp['received'] == rr_values.length,
         "received=#{resp['received']}")

  puts "  Waiting for extreme RR post-processing (polling every 5s, up to 90s)..."
  ok, processed_count = poll_until(timeout_seconds: 90, interval_seconds: 5) do
    r = influx.query(
      "SELECT count(rr_clean) FROM polar_raw " \
      "WHERE \"device\" = '#{TEST_DEVICE}' AND recording_id = '#{recording_id}' AND rr_clean > 0"
    )
    count = if r.empty? || r.first['values'].nil?
              0
            else
              r.first['values'].first['count']
            end
    count >= 120 ? count : nil
  end

  processed_count ||= 0
  assert("Extreme RR batch processed (rr_clean present)", ok, "rr_clean_count=#{processed_count}")

  if ok
    r = influx.query(
      "SELECT rr_interval, rr_clean, artifact_type FROM polar_raw " \
      "WHERE \"device\" = '#{TEST_DEVICE}' AND recording_id = '#{recording_id}' " \
      "AND (rr_interval < 300 OR rr_interval > 2200)"
    )
    rows = (r.empty? || r.first['values'].nil?) ? [] : r.first['values']
    assert("All extreme points found", rows.length == 4, "found=#{rows.length}")

    flagged = rows.all? { |v| !v['artifact_type'].nil? && v['artifact_type'] != 'none' }
    bounded = rows.all? do |v|
      rc = v['rr_clean']
      !rc.nil? && rc >= 300 && rc <= 2200
    end
    assert("Extreme points are classified as artifacts", flagged,
           "types=#{rows.map { |v| v['artifact_type'] }}")
    assert("Extreme points are corrected into hard bounds", bounded,
           "rr_clean=#{rows.map { |v| v['rr_clean'] }}")

    min_ts = beats.first[:timestamp]
    max_ts = beats.last[:timestamp] + 120_000
    q = influx.query(
      "SELECT extreme_rr_count FROM polar_quality_1m " \
      "WHERE \"device\" = '#{TEST_DEVICE}' AND time >= #{min_ts}ms AND time <= #{max_ts}ms"
    )
    quality_rows = (q.empty? || q.first['values'].nil?) ? [] : q.first['values']
    total_extreme = quality_rows.map { |v| v['extreme_rr_count'] || 0 }.sum
    assert("Quality metrics include extreme_rr_count", total_extreme >= 4,
           "total_extreme_rr_count=#{total_extreme}")
  end
end

# ── Test 12: Post-processor — weak longshort demotion ────────────────────────

test("Post-processor: weak longshort corrections are demoted") do
  influx.query("DELETE FROM polar_raw WHERE \"device\" = '#{TEST_DEVICE}'")
  influx.query("DELETE FROM polar_quality_1m WHERE \"device\" = '#{TEST_DEVICE}'")
  sleep 0.5

  weak_start = BASE_TS + 2_100_000
  recording_id = 'rec-weak-longshort'

  rr_values = [
    1007, 989, 1011, 971, 897, 858, 1018, 1007, 873, 906, 922, 958, 921, 866, 1012, 945, 920, 931, 982, 875,
    943, 1012, 310, 900, 951, 881, 855, 860, 1006, 979, 876, 894, 890, 1001, 851, 934, 852, 941, 851, 939,
    1005, 890, 862, 878, 943, 920, 988, 966, 936, 963, 941, 977, 885, 893, 3208, 1004, 877, 1033, 896, 893,
    888, 884, 1026, 881, 978, 916, 1027, 963, 883, 1023, 858, 1007, 967, 1003, 854, 917, 1008, 1024, 905, 1020,
    1022, 949, 952, 1024, 984, 949, 1023, 978, 888, 962, 916, 974, 906, 1064, 942, 986, 1045, 975, 917, 910,
    1013, 956, 879, 1028, 998, 1018, 914, 984, 930, 969, 890, 1024, 953, 991, 978, 998, 1035, 970, 930, 923,
    991, 979, 836, 935, 947, 862, 895, 950, 999, 984, 897, 976, 1004, 977, 1013, 1000, 897, 901, 904, 996,
    949, 858, 921, 904, 966, 947, 955, 974, 918, 919, 879, 921, 872, 896, 962, 874, 1008, 864, 1001, 318,
    981, 883, 956, 873, 1025, 925, 929, 914, 1011, 949, 872, 943, 1014, 879, 901, 965, 960, 990, 966, 993
  ]

  beats = []
  ts = weak_start
  rr_values.each_with_index do |rr, i|
    beats << {
      recordingId: recording_id,
      recordingSeq: i + 1,
      timestamp: ts,
      rrInterval: rr.to_f
    }
    ts += rr
  end

  resp = post_json('/beats/batch', {
    source: TEST_SOURCE,
    device: TEST_DEVICE,
    batchType: 'recording',
    uploadId: next_upload_id('rec-weak'),
    beats: beats
  })
  assert("Weak-correction batch accepted", resp['received'] == rr_values.length,
         "received=#{resp['received']}")

  puts "  Waiting for weak-correction post-processing (polling every 5s, up to 90s)..."
  ok, processed_count = poll_until(timeout_seconds: 90, interval_seconds: 5) do
    r = influx.query(
      "SELECT count(rr_clean) FROM polar_raw " \
      "WHERE \"device\" = '#{TEST_DEVICE}' AND recording_id = '#{recording_id}' AND rr_clean > 0"
    )
    count = if r.empty? || r.first['values'].nil?
              0
            else
              r.first['values'].first['count']
            end
    count >= 140 ? count : nil
  end
  assert("Weak-correction batch processed", ok, "rr_clean_count=#{processed_count || 0}")

  if ok
    weak = influx.query(
      "SELECT count(weak_correction) FROM polar_raw " \
      "WHERE \"device\" = '#{TEST_DEVICE}' AND recording_id = '#{recording_id}' " \
      "AND weak_correction = true"
    )
    weak_count = if weak.empty? || weak.first['values'].nil?
                   0
                 else
                   weak.first['values'].first['count']
                 end
    assert("Weak corrections are explicitly marked", weak_count > 0, "weak_count=#{weak_count}")

    tiny = influx.query(
      "SELECT count(correction_delta_ms) FROM polar_raw " \
      "WHERE \"device\" = '#{TEST_DEVICE}' AND recording_id = '#{recording_id}' " \
      "AND artifact_type = 'longshort' " \
      "AND correction_applied = true " \
      "AND correction_delta_ms >= -5 AND correction_delta_ms <= 5"
    )
    tiny_count = if tiny.empty? || tiny.first['values'].nil?
                   0
                 else
                   tiny.first['values'].first['count']
                 end
    assert("No tiny-delta longshort corrections remain", tiny_count == 0, "tiny_longshort=#{tiny_count}")
  end
end

# ── Test 13: Post-processor — correction burst quality flags ─────────────────

test("Post-processor: correction bursts set low-confidence quality flags") do
  influx.query("DELETE FROM polar_raw WHERE \"device\" = '#{TEST_DEVICE}'")
  influx.query("DELETE FROM polar_quality_1m WHERE \"device\" = '#{TEST_DEVICE}'")
  sleep 0.5

  burst_start = BASE_TS + 2_400_000
  recording_id = 'rec-correction-burst'
  rr_values = Array.new(180) do |i|
    case i % 3
    when 0 then 250.0
    when 1 then 2500.0
    else 900.0
    end
  end

  beats = []
  ts = burst_start
  rr_values.each_with_index do |rr, i|
    beats << {
      recordingId: recording_id,
      recordingSeq: i + 1,
      timestamp: ts,
      rrInterval: rr
    }
    ts += rr.to_i
  end

  resp = post_json('/beats/batch', {
    source: TEST_SOURCE,
    device: TEST_DEVICE,
    batchType: 'recording',
    uploadId: next_upload_id('rec-burst'),
    beats: beats
  })
  assert("Burst batch accepted", resp['received'] == rr_values.length,
         "received=#{resp['received']}")

  puts "  Waiting for correction-burst quality metrics (polling every 5s, up to 90s)..."
  ok, quality_rows = poll_until(timeout_seconds: 90, interval_seconds: 5) do
    q = influx.query(
      "SELECT correction_rate, max_consecutive_corrected, correction_burst_level, low_confidence " \
      "FROM polar_quality_1m WHERE \"device\" = '#{TEST_DEVICE}' " \
      "AND time >= #{burst_start}ms AND time <= #{burst_start + 600_000}ms"
    )
    rows = (q.empty? || q.first['values'].nil?) ? [] : q.first['values']
    has_flag = rows.any? do |v|
      level = v['correction_burst_level']
      rate = v['correction_rate'] || 0
      low = v['low_confidence'] == true
      low && (level == 'warning' || level == 'critical') && rate >= 0.2
    end
    has_flag ? rows : nil
  end
  assert("Burst windows are flagged as low confidence", ok, "quality_rows=#{quality_rows&.length || 0}")
end

# ── Test 14: Post-processor — hr_clean ───────────────────────────────────────

test("Post-processor: hr_clean") do
  r = influx.query(
    "SELECT hr_clean FROM polar_raw " \
    "WHERE \"device\" = '#{TEST_DEVICE}' AND hr_clean > 0 LIMIT 5"
  )
  hr_count = r.empty? || r.first['values'].nil? ? 0 : r.first['values'].length
  assert("hr_clean > 0 on processed beats", hr_count > 0, "Found #{hr_count}")
end

# ── Test 15: 5-min summaries ─────────────────────────────────────────────────

test("5-min summaries") do
  r = influx.query(
    "SELECT rmssd, sample_count FROM polar_hrv_summary " \
    "WHERE \"device\" = '#{TEST_DEVICE}'"
  )
  summary_count = r.empty? || r.first['values'].nil? ? 0 : r.first['values'].length
  assert("At least 1 summary written", summary_count >= 1,
         "Found #{summary_count} summaries")

  if summary_count > 0
    values = r.first['values']
    has_rmssd = values.any? { |v| !v['rmssd'].nil? }
    has_count = values.any? { |v| v['sample_count'] && v['sample_count'] >= 10 }
    assert("Summary has rmssd field", has_rmssd,
           "rmssd values=#{values.map { |v| v['rmssd'] }}")
    assert("Summary has sample_count >= 10", has_count,
           "sample_count values=#{values.map { |v| v['sample_count'] }}")

    ar = influx.query(
      "SELECT max(artifact_count) FROM polar_hrv_summary " \
      "WHERE \"device\" = '#{TEST_DEVICE}'"
    )
    max_artifacts = if ar.empty? || ar.first['values'].nil? || ar.first['values'].empty?
                      0
                    else
                      ar.first['values'].first['max']
                    end
    assert("Summary artifact_count reflects detected artifacts", max_artifacts && max_artifacts > 0,
           "max_artifact_count=#{max_artifacts}")
  end
end

# ── Test 16: 1-min quality metrics ───────────────────────────────────────────

test("1-min quality metrics") do
  r = influx.query(
    "SELECT sample_count, artifact_count, artifact_rate, " \
    "unclassified_corrected_count, out_of_order_count, extreme_rr_count, " \
    "weak_correction_count, corrected_count, correction_rate, " \
    "max_consecutive_corrected, correction_burst_level, low_confidence " \
    "FROM polar_quality_1m WHERE \"device\" = '#{TEST_DEVICE}'"
  )
  quality_count = r.empty? || r.first['values'].nil? ? 0 : r.first['values'].length
  assert("At least 1 quality point written", quality_count >= 1, "Found #{quality_count}")

  if quality_count > 0
    values = r.first['values']
    has_sample = values.any? { |v| v['sample_count'] && v['sample_count'] > 0 }
    has_artifact_count = values.any? { |v| !v['artifact_count'].nil? }
    has_artifact_rate = values.any? { |v| !v['artifact_rate'].nil? }
    has_correction_rate = values.any? { |v| !v['correction_rate'].nil? }
    has_burst_level = values.any? { |v| !v['correction_burst_level'].nil? }
    assert("Quality sample_count > 0 exists", has_sample)
    assert("Quality artifact_count field exists", has_artifact_count)
    assert("Quality artifact_rate field exists", has_artifact_rate)
    assert("Quality correction_rate field exists", has_correction_rate)
    assert("Quality correction_burst_level field exists", has_burst_level)
  end
end

# ── Test 17: Cleanup ─────────────────────────────────────────────────────────

test("Cleanup") do
  influx.query("DELETE FROM polar_raw WHERE \"device\" = '#{TEST_DEVICE}'")
  influx.query("DELETE FROM polar_realtime WHERE \"device\" = '#{TEST_DEVICE}'")
  influx.query("DELETE FROM polar_hrv_summary WHERE \"device\" = '#{TEST_DEVICE}'")
  influx.query("DELETE FROM polar_quality_1m WHERE \"device\" = '#{TEST_DEVICE}'")
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
