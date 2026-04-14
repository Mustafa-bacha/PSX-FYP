import { Navigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext.jsx';

export default function AuthCallback() {
  const { loading, isAuthenticated } = useAuth();

  if (loading) {
    return (
      <div className="max-w-md mx-auto px-4 py-16 text-center">
        <h1 className="text-xl font-semibold text-white">Completing Google sign-in…</h1>
        <p className="text-slate-400 text-sm mt-2">Please wait while we verify your session.</p>
      </div>
    );
  }

  return isAuthenticated
    ? <Navigate to="/" replace />
    : <Navigate to="/login" replace />;
}
