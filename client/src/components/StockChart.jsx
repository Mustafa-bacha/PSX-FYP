import { useMemo } from 'react';
import {
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Area,
  ComposedChart,
  ReferenceLine,
  Line,
  Bar,
  LineChart,
} from 'recharts';

function computeEMA(values = [], period = 14) {
  const k = 2 / (period + 1);
  let prev = null;
  return values.map((v) => {
    if (!Number.isFinite(v)) return null;
    if (prev == null) {
      prev = v;
      return v;
    }
    prev = v * k + prev * (1 - k);
    return prev;
  });
}

function computeRSI(values = [], period = 14) {
  if (!Array.isArray(values) || values.length < period + 1) return values.map(() => null);

  const rsi = values.map(() => null);
  let gainSum = 0;
  let lossSum = 0;

  for (let i = 1; i <= period; i += 1) {
    const diff = values[i] - values[i - 1];
    if (diff >= 0) gainSum += diff;
    else lossSum += Math.abs(diff);
  }

  let avgGain = gainSum / period;
  let avgLoss = lossSum / period;

  const firstRS = avgLoss === 0 ? 100 : avgGain / avgLoss;
  rsi[period] = 100 - (100 / (1 + firstRS));

  for (let i = period + 1; i < values.length; i += 1) {
    const diff = values[i] - values[i - 1];
    const gain = diff > 0 ? diff : 0;
    const loss = diff < 0 ? Math.abs(diff) : 0;

    avgGain = ((avgGain * (period - 1)) + gain) / period;
    avgLoss = ((avgLoss * (period - 1)) + loss) / period;

    const rs = avgLoss === 0 ? 100 : avgGain / avgLoss;
    rsi[i] = 100 - (100 / (1 + rs));
  }

  return rsi;
}

function computeMACD(values = []) {
  const ema12 = computeEMA(values, 12);
  const ema26 = computeEMA(values, 26);
  const macd = values.map((_, i) => (
    Number.isFinite(ema12[i]) && Number.isFinite(ema26[i])
      ? ema12[i] - ema26[i]
      : null
  ));
  const signal = computeEMA(macd, 9);
  const histogram = macd.map((m, i) => (
    Number.isFinite(m) && Number.isFinite(signal[i]) ? m - signal[i] : null
  ));

  return { macd, signal, histogram };
}

function computeATR(rows = [], period = 14) {
  if (!Array.isArray(rows) || rows.length === 0) return [];

  const trueRanges = rows.map((row, idx) => {
    const high = Number(row.high);
    const low = Number(row.low);
    const prevClose = idx > 0 ? Number(rows[idx - 1].close) : null;
    if (!Number.isFinite(high) || !Number.isFinite(low)) return null;

    if (!Number.isFinite(prevClose)) return high - low;
    return Math.max(
      high - low,
      Math.abs(high - prevClose),
      Math.abs(low - prevClose),
    );
  });

  const atr = rows.map(() => null);
  let sum = 0;

  for (let i = 0; i < trueRanges.length; i += 1) {
    const tr = trueRanges[i];
    if (!Number.isFinite(tr)) continue;

    if (i < period) {
      sum += tr;
      if (i === period - 1) atr[i] = sum / period;
    } else {
      atr[i] = ((atr[i - 1] ?? tr) * (period - 1) + tr) / period;
    }
  }

  return atr;
}

export default function StockChart({
  data = [],
  height = 280,
  indicators = {
    sma: true,
    ema: true,
    bollinger: false,
    rsi: false,
    macd: false,
    volume: true,
    atr: false,
  },
}) {
  const chartData = useMemo(() => {
    if (!data || data.length === 0) return [];

    const closes = data.map((d) => Number(d.close));
    const rsi = computeRSI(closes, 14);
    const macdPack = computeMACD(closes);
  const atr = computeATR(data, 14);

    const processed = data.map((d) => ({
      ...d,
      dateShort: d.date ? d.date.slice(5) : '',
      fullDate: d.date || '',
    }));

    if (processed.length > 1) {
      const first = processed[0].close;
      const last = processed[processed.length - 1].close;
      const change = last - first;
      const changePercent = ((change / first) * 100).toFixed(2);

      return processed.map((d, idx) => ({
        ...d,
        change: d.close - first,
        changePercent: idx === processed.length - 1 ? changePercent : null,
        rsi_14: rsi[idx],
        macd: macdPack.macd[idx],
        macd_signal: macdPack.signal[idx],
        macd_hist: macdPack.histogram[idx],
        atr_14: atr[idx],
      }));
    }

    return processed;
  }, [data]);

  if (!chartData.length) {
    return (
      <div className="h-[280px] flex items-center justify-center text-slate-500 bg-slate-800/30 rounded-lg">
        <div className="text-center">
          <p className="text-sm">No chart data available</p>
          <p className="text-xs text-slate-600 mt-1">Data will appear once available</p>
        </div>
      </div>
    );
  }

  const firstPrice = chartData[0]?.close || 0;
  const lastPrice = chartData[chartData.length - 1]?.close || 0;
  const priceChange = lastPrice - firstPrice;
  const priceChangePercent = firstPrice ? ((priceChange / firstPrice) * 100).toFixed(2) : 0;
  const isPositive = priceChange >= 0;

  const mainSeriesValues = chartData.flatMap((d) => {
    const vals = [Number(d.close)];
    if (indicators?.sma) vals.push(Number(d.ma_20), Number(d.ma_50));
    if (indicators?.ema) vals.push(Number(d.ema_20));
    if (indicators?.bollinger) vals.push(Number(d.bb_upper), Number(d.bb_lower));
    return vals.filter((v) => Number.isFinite(v));
  });

  const minPrice = mainSeriesValues.length ? Math.min(...mainSeriesValues) : 0;
  const maxPrice = mainSeriesValues.length ? Math.max(...mainSeriesValues) : 1;
  const priceRange = maxPrice - minPrice;
  const yAxisPadding = Math.max(1, priceRange * 0.1);

  const CustomTooltip = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      const row = payload[0].payload;
      return (
        <div className="bg-slate-800/95 backdrop-blur-sm border border-slate-600 rounded-lg shadow-xl p-3">
          <p className="text-slate-400 text-xs mb-2 font-medium">{row.fullDate || label}</p>
          <div className="space-y-1">
            <div className="flex items-center justify-between gap-4">
              <span className="text-slate-400 text-xs">Close:</span>
              <span className="text-white font-semibold">PKR {Number(row.close).toFixed(2)}</span>
            </div>
          </div>
        </div>
      );
    }
    return null;
  };

  return (
    <div className="relative space-y-4">
      <div className="absolute top-2 left-2 z-10 bg-slate-800/90 backdrop-blur-sm border border-slate-700/50 rounded-lg px-3 py-2">
        <div className="flex items-baseline gap-2">
          <span className="text-slate-400 text-xs">Latest:</span>
          <span className="text-white font-bold text-lg">PKR {lastPrice.toFixed(2)}</span>
          <span className={`text-xs font-semibold ${isPositive ? 'text-green-400' : 'text-red-400'}`}>
            {isPositive ? '+' : ''}{priceChange.toFixed(2)} ({isPositive ? '+' : ''}{priceChangePercent}%)
          </span>
        </div>
      </div>

      <ResponsiveContainer width="100%" height={height}>
        <ComposedChart data={chartData} margin={{ top: 20, right: 10, left: 0, bottom: 5 }}>
          <defs>
            <linearGradient id="colorGradient" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={isPositive ? '#22c55e' : '#ef4444'} stopOpacity={0.3} />
              <stop offset="100%" stopColor={isPositive ? '#22c55e' : '#ef4444'} stopOpacity={0.05} />
            </linearGradient>
          </defs>
          <XAxis
            dataKey="dateShort"
            stroke="#64748b"
            tick={{ fontSize: 10, fill: '#94a3b8' }}
            axisLine={{ stroke: '#475569' }}
            tickLine={{ stroke: '#475569' }}
          />
          <YAxis
            domain={[minPrice - yAxisPadding, maxPrice + yAxisPadding]}
            stroke="#64748b"
            tick={{ fontSize: 10, fill: '#94a3b8' }}
            axisLine={{ stroke: '#475569' }}
            tickLine={{ stroke: '#475569' }}
            tickFormatter={(value) => value.toFixed(0)}
          />
          <Tooltip content={<CustomTooltip />} />
          <ReferenceLine
            y={firstPrice}
            stroke="#64748b"
            strokeDasharray="3 3"
            strokeOpacity={0.5}
            label={{ value: 'Start', position: 'right', fill: '#94a3b8', fontSize: 10 }}
          />
          <Area
            type="monotone"
            dataKey="close"
            stroke={isPositive ? '#22c55e' : '#ef4444'}
            strokeWidth={2.5}
            fill="url(#colorGradient)"
            dot={false}
            activeDot={{ r: 4, fill: isPositive ? '#22c55e' : '#ef4444', strokeWidth: 2, stroke: '#fff' }}
          />
          {indicators?.ema && (
            <Line
              type="monotone"
              dataKey="ema_20"
              stroke="#f59e0b"
              strokeWidth={1.6}
              dot={false}
              name="EMA 20"
            />
          )}
          {indicators?.sma && (
            <>
              <Line type="monotone" dataKey="ma_20" stroke="#2dd4bf" strokeWidth={1.4} dot={false} name="SMA 20" />
              <Line type="monotone" dataKey="ma_50" stroke="#60a5fa" strokeWidth={1.2} dot={false} strokeDasharray="5 4" name="SMA 50" />
            </>
          )}
          {indicators?.bollinger && (
            <>
              <Line type="monotone" dataKey="bb_upper" stroke="#38bdf8" strokeWidth={1.1} dot={false} strokeDasharray="4 3" />
              <Line type="monotone" dataKey="bb_mid" stroke="#94a3b8" strokeWidth={1} dot={false} strokeDasharray="2 2" />
              <Line type="monotone" dataKey="bb_lower" stroke="#38bdf8" strokeWidth={1.1} dot={false} strokeDasharray="4 3" />
            </>
          )}
        </ComposedChart>
      </ResponsiveContainer>

      {indicators?.volume && (
        <div className="border border-slate-700/50 rounded-lg p-3 bg-slate-900/20">
          <p className="text-slate-300 text-xs mb-2 font-semibold">Volume</p>
          <div className="h-24">
            <ResponsiveContainer width="100%" height="100%">
              <ComposedChart data={chartData}>
                <XAxis dataKey="dateShort" stroke="#64748b" tick={{ fontSize: 9, fill: '#94a3b8' }} />
                <YAxis stroke="#64748b" tick={{ fontSize: 9, fill: '#94a3b8' }} width={38} />
                <Tooltip contentStyle={{ backgroundColor: '#0f172a', borderColor: '#475569' }} />
                <Bar dataKey="volume" fill="#22c55e" maxBarSize={8} />
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {indicators?.rsi && (
        <div className="border border-slate-700/50 rounded-lg p-3 bg-slate-900/20">
          <p className="text-slate-300 text-xs mb-2 font-semibold">RSI (14)</p>
          <div className="h-28">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData}>
                <XAxis dataKey="dateShort" stroke="#64748b" tick={{ fontSize: 9, fill: '#94a3b8' }} />
                <YAxis domain={[0, 100]} stroke="#64748b" tick={{ fontSize: 9, fill: '#94a3b8' }} width={30} />
                <Tooltip contentStyle={{ backgroundColor: '#0f172a', borderColor: '#475569' }} />
                <ReferenceLine y={70} stroke="#ef4444" strokeDasharray="3 3" />
                <ReferenceLine y={30} stroke="#22c55e" strokeDasharray="3 3" />
                <Line type="monotone" dataKey="rsi_14" stroke="#a78bfa" strokeWidth={1.8} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {indicators?.macd && (
        <div className="border border-slate-700/50 rounded-lg p-3 bg-slate-900/20">
          <p className="text-slate-300 text-xs mb-2 font-semibold">MACD (12, 26, 9)</p>
          <div className="h-32">
            <ResponsiveContainer width="100%" height="100%">
              <ComposedChart data={chartData}>
                <XAxis dataKey="dateShort" stroke="#64748b" tick={{ fontSize: 9, fill: '#94a3b8' }} />
                <YAxis stroke="#64748b" tick={{ fontSize: 9, fill: '#94a3b8' }} width={38} />
                <Tooltip contentStyle={{ backgroundColor: '#0f172a', borderColor: '#475569' }} />
                <ReferenceLine y={0} stroke="#64748b" />
                <Bar dataKey="macd_hist" fill="#334155" maxBarSize={7} />
                <Line type="monotone" dataKey="macd" stroke="#22d3ee" dot={false} strokeWidth={1.5} />
                <Line type="monotone" dataKey="macd_signal" stroke="#f59e0b" dot={false} strokeWidth={1.4} />
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {indicators?.atr && (
        <div className="border border-slate-700/50 rounded-lg p-3 bg-slate-900/20">
          <p className="text-slate-300 text-xs mb-2 font-semibold">ATR (14)</p>
          <div className="h-24">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData}>
                <XAxis dataKey="dateShort" stroke="#64748b" tick={{ fontSize: 9, fill: '#94a3b8' }} />
                <YAxis stroke="#64748b" tick={{ fontSize: 9, fill: '#94a3b8' }} width={38} />
                <Tooltip contentStyle={{ backgroundColor: '#0f172a', borderColor: '#475569' }} />
                <Line type="monotone" dataKey="atr_14" stroke="#f97316" dot={false} strokeWidth={1.7} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}
    </div>
  );
}
