export function getPredictionColor(direction) {
  if (!direction) return 'text-slate-400';
  switch (direction) {
    case 'Up':
      return 'text-green-400';
    case 'Down':
      return 'text-red-400';
    default:
      return 'text-slate-400';
  }
}

export function getRecommendationColor(recommendation) {
  if (!recommendation) return 'text-amber-400';
  switch (recommendation) {
    case 'Buy':
      return 'text-green-400';
    case 'Sell':
      return 'text-red-400';
    default:
      return 'text-amber-400';
  }
}
