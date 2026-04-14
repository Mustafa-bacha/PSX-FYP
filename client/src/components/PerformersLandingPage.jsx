import React, { useEffect, useMemo, useState } from 'react';
import { apiClient } from '../api/client.js';
import { MarketPerformers } from './MarketPerformers.jsx';

export function PerformersLandingPage({ onOpenDashboard }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const loadPerformers = async (refresh = false) => {
    setLoading(true);
    setError('');
    try {
      const payload = await apiClient.performers(refresh);
      setData(payload);
    } catch (err) {
      setError(err.message || 'Failed to load market performers');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadPerformers(false);

    const timer = setInterval(() => {
      if (!document.hidden) loadPerformers(false);
    }, 5 * 60 * 1000);

    return () => clearInterval(timer);
  }, []);

  const meta = useMemo(() => {
    if (!data) return 'Loading...';
    return [
      data.as_of ? `As of ${data.as_of}` : null,
      data.fetched_at ? `Fetched ${new Date(data.fetched_at).toLocaleString()}` : null,
      data.cache_status ? `Cache: ${data.cache_status}` : null,
      'Auto-refresh: 5 min'
    ].filter(Boolean).join(' • ');
  }, [data]);

  return (
    <section className="ad-section">
      <div className="ad-card">
        <div>
          <h2 className="ad-h2">Market Performers</h2>
          <p className="ad-p">
            Live PSX top active/advancers/decliners sourced from DPS and refreshed every 5 minutes.
          </p>
        </div>
        <button className="ad-btn ad-btn-primary" onClick={onOpenDashboard}>Go to Dashboard</button>
      </div>

      {error ? <div className="ad-alert ad-alert-danger">{error}</div> : null}

      <MarketPerformers
        data={data?.performers}
        loading={loading}
        onRefresh={() => loadPerformers(true)}
        meta={meta}
      />
    </section>
  );
}
