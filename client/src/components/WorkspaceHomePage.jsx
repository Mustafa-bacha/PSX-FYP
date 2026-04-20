import React, { useEffect, useMemo, useState } from 'react';
import { apiClient } from '../api/client.js';

export function WorkspaceHomePage({ user, onOpenMarket, onOpenDashboard, onOpenSimulator, onOpenStrategy, onOpenHelp }) {
  const [news, setNews] = useState([]);
  const [loadingNews, setLoadingNews] = useState(true);
  const [newsError, setNewsError] = useState('');

  const loadDailyNews = async (forceRefresh = false) => {
    setLoadingNews(true);
    setNewsError('');
    try {
      const payload = await apiClient.dailyNews(8, forceRefresh);
      const nextItems = Array.isArray(payload?.items) ? payload.items : [];
      setNews(nextItems);
    } catch (err) {
      setNewsError(String(err?.message || err || 'Failed to load daily news'));
      setNews([]);
    } finally {
      setLoadingNews(false);
    }
  };

  useEffect(() => {
    loadDailyNews();
  }, []);

  const hasNews = news.length > 0;

  const updatedTime = useMemo(() => {
    if (!hasNews || !news[0]?.analyzed_at) return '';
    const dt = new Date(news[0].analyzed_at);
    if (Number.isNaN(dt.getTime())) return '';
    return dt.toLocaleString();
  }, [hasNews, news]);

  const sourceLabel = (source) => String(source || 'news').replace(/_/g, ' ');

  const summaryText = (item) => {
    const detailed = String(item?.summary || '').trim();
    if (detailed) return detailed;

    const headline = String(item?.headline || '').trim();
    if (!headline) return 'No summary available for this update.';

    const source = sourceLabel(item?.source);
    return `${headline}. This update is sourced from ${source} and reflects the latest business news signal captured by the platform. Use it as a quick context check before deeper market analysis.`;
  };

  return (
    <section className="page-stack">
  <div className="card hero-card home-hero-fixed">
        <div>
          <h2>Welcome back, {user?.full_name || user?.email || 'Trader'} 👋</h2>
          <p className="muted-line">
            Explore market movers, run AI-assisted analysis, and practice risk-free paper trading in one workspace.
          </p>
        </div>
        <div className="hero-actions">
          <button className="toggle-btn active" onClick={onOpenDashboard}>Open Dashboard</button>
          <button className="toggle-btn" onClick={onOpenMarket}>View Market</button>
        </div>
      </div>



      <article className="card daily-news-card">
        <div className="section-head">
          <div>
            <h3>Daily Pakistan Hot News</h3>
            <p className="muted-line">Freshly scraped business headlines from Pakistan sources.</p>
          </div>
          <div className="daily-news-actions">
            {updatedTime ? <small className="muted-line">Updated: {updatedTime}</small> : null}
            <button className="toggle-btn" onClick={() => loadDailyNews(true)} disabled={loadingNews}>
              {loadingNews ? 'Refreshing…' : 'Refresh'}
            </button>
          </div>
        </div>

        {newsError ? <div className="error-box">{newsError}</div> : null}

        {loadingNews && !hasNews ? (
          <p className="muted-line">Loading daily hot news…</p>
        ) : null}

        {!loadingNews && !newsError && !hasNews ? (
          <p className="muted-line">No fresh headlines available yet. Run sentiment cycle to ingest new news.</p>
        ) : null}

        {hasNews ? (
          <ul className="daily-news-list">
            {news.map((item, idx) => (
              <li key={`${item.headline}-${idx}`}>
                <div className="daily-news-topline">
                  <b className={String(item.label || 'neutral').toLowerCase()}>{String(item.label || 'neutral')}</b>
                  <small>{sourceLabel(item.source)}</small>
                </div>
                <p>{item.headline}</p>
                <p className="daily-news-summary expanded" style={{ whiteSpace: 'pre-line' }}>
                  {summaryText(item)}
                </p>
              </li>
            ))}
          </ul>
        ) : null}
      </article>
    </section>
  );
}
