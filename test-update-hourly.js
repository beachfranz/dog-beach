// test-update-hourly.js
// Usage: SUPABASE_URL=https://<project>.supabase.co SUPABASE_ANON_KEY=<anon_key> node test-update-hourly.js

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;

if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
  console.error('Please set SUPABASE_URL and SUPABASE_ANON_KEY environment variables.');
  process.exit(1);
}

const FUNCTION_URL = `${SUPABASE_URL.replace(/\\/$/, '')}/functions/v1/update-hourly-details`;
const POLL_INTERVAL_MS = 3000;
const POLL_TIMEOUT_MS = 120000; // 2 minutes

async function callEdgeFunction(payload) {
  const res = await fetch(FUNCTION_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${SUPABASE_ANON_KEY}`,
    },
    body: JSON.stringify(payload),
  });
  const txt = await res.text();
  let json;
  try { json = txt ? JSON.parse(txt) : null; } catch (e) { json = txt; }
  return { status: res.status, body: json };
}

async function pollHourlyDetails(startISO, endISO, lat, lon) {
  const url = new URL(`${SUPABASE_URL.replace(/\\/$/, '')}/rest/v1/hourly_details`);
  // filter by timestamp range; adjust column names if different
  url.searchParams.set('select', '*');
  url.searchParams.set('timestamp', `gte.${startISO}`);
  url.searchParams.append('timestamp', `lte.${endISO}`);
  if (lat !== undefined && lat !== null) {
    url.searchParams.append('latitude', `eq.${lat}`);
  }
  if (lon !== undefined && lon !== null) {
    url.searchParams.append('longitude', `eq.${lon}`);
  }
  // limit results returned
  url.searchParams.set('limit', '1000');

  const headers = {
    apikey: SUPABASE_ANON_KEY,
    Authorization: `Bearer ${SUPABASE_ANON_KEY}`,
  };

  const start = Date.now();
  while (Date.now() - start < POLL_TIMEOUT_MS) {
    const res = await fetch(url.toString(), { headers });
    if (!res.ok) {
      const txt = await res.text();
      throw new Error(`Failed fetching hourly_details: ${res.status} ${txt}`);
    }
    const rows = await res.json();
    if (Array.isArray(rows) && rows.length > 0) {
      return rows;
    }
    await new Promise((r) => setTimeout(r, POLL_INTERVAL_MS));
  }
  return []; // timed out
}

(async () => {
  try {
    // Example payload: set start/end to last 24 hours
    const end = new Date();
    const start = new Date(end.getTime() - 24 * 3600 * 1000);
    const payload = {
      // optional: location_id: 'your-location-id',
      start: start.toISOString(),
      end: end.toISOString(),
      // optional: noaa_station_id: '9410580',
      source: 'test-script',
    };

    console.log('Calling Edge Function at', FUNCTION_URL);
    const callRes = await callEdgeFunction(payload);
    console.log('Edge function response:', callRes.status, callRes.body);

    if (callRes.status !== 202) {
      console.error('Edge function did not accept the request. Exiting.');
      process.exit(2);
    }

    console.log('Polling hourly_details for upserted rows (this may take up to 2 minutes)...');

    // For polling we query by timestamp range only; if you want to target a specific lat/lon include them
    const rows = await pollHourlyDetails(payload.start, payload.end, null, null);

    if (!rows || rows.length === 0) {
      console.warn('No rows found in hourly_details within timeout. Either background job still running or upsert failed.');
      process.exit(3);
    }

    console.log(`Found ${rows.length} rows in hourly_details (sample 5):`);
    console.log(rows.slice(0, 5));
    process.exit(0);
  } catch (err) {
    console.error('Test failed:', err);
    process.exit(4);
  }
})();
