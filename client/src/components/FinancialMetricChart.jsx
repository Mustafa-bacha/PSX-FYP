import { ResponsiveContainer, CartesianGrid, XAxis, YAxis, Tooltip, BarChart, Bar } from 'recharts';
import { cardClass } from '../lib/constants.js';

function formatCompact(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return '—';
  if (Math.abs(n) >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(2)}B`;
  if (Math.abs(n) >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`;
  if (Math.abs(n) >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return `${n.toFixed(2)}`;
}

export default function FinancialMetricChart({ title, points = [] }) {
  if (!Array.isArray(points) || points.length === 0) {
    return (
      <div className={`${cardClass} p-5`}>
        <h3 className="text-white font-semibold mb-2">{title}</h3>
        <p className="text-slate-500 text-sm">No financial series available.</p>
      </div>
    );
  }

  return (
    <div className={`${cardClass} p-5`}>
      <h3 className="text-white font-semibold mb-3">{title}</h3>
      <div className="h-52">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={points} margin={{ top: 10, right: 8, left: 0, bottom: 6 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#334155" opacity={0.5} />
            <XAxis dataKey="period" stroke="#94a3b8" tick={{ fontSize: 10 }} />
            <YAxis stroke="#94a3b8" tick={{ fontSize: 10 }} tickFormatter={(v) => formatCompact(v)} width={44} />
            <Tooltip
              cursor={{ fill: 'rgba(148, 163, 184, 0.08)' }}
              contentStyle={{
                backgroundColor: 'rgba(15, 23, 42, 0.95)',
                borderColor: '#475569',
                borderRadius: '8px',
                color: '#e2e8f0'
              }}
              formatter={(v) => [`${formatCompact(v)}`, title]}
            />
            <Bar dataKey="value" fill="#3b82f6" radius={[6, 6, 0, 0]} maxBarSize={44} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
