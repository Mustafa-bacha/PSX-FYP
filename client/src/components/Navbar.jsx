import { NavLink, useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext.jsx';

const baseLink = 'text-slate-400 hover:text-white text-sm font-medium transition-colors relative group';

function TopLink({ to, children }) {
  return (
    <NavLink
      to={to}
      className={({ isActive }) => `${baseLink} ${isActive ? 'text-white' : ''}`}
    >
      {children}
      <span className="absolute bottom-0 left-0 w-0 h-0.5 bg-brand-400 group-hover:w-full transition-all duration-300"></span>
    </NavLink>
  );
}

export default function Navbar() {
  const { user, logout, isAuthenticated } = useAuth();
  const navigate = useNavigate();

  const handleLogout = () => {
    logout();
    navigate('/');
  };

  return (
    <header className="border-b border-slate-700/50 bg-slate-900/80 backdrop-blur-md sticky top-0 z-50 shadow-lg">
      <div className="max-w-6xl mx-auto px-4 h-16 flex items-center justify-between gap-6">
        <NavLink to={isAuthenticated ? '/' : '/login'} className="font-bold text-2xl text-brand-400 tracking-tight hover:text-brand-300 transition-colors flex items-center gap-2">
          <span className="bg-gradient-to-r from-brand-400 to-brand-600 bg-clip-text text-transparent">
            Stock-In-Sight
          </span>
        </NavLink>

        <nav className="flex items-center gap-4 md:gap-6">
          {isAuthenticated ? (
            <>
              <TopLink to="/">Home</TopLink>
              <TopLink to="/chat">Chat</TopLink>
              <TopLink to="/sentiment-analysis">Daily Pakistan Business News</TopLink>
              <TopLink to="/dashboard">Dashboard</TopLink>
              <TopLink to="/watch-list">Watch List</TopLink>
              <TopLink to="/simulator">Simulator</TopLink>
              <span className="text-slate-500 text-xs font-mono px-2 py-1 bg-slate-800/50 rounded max-w-32 truncate">{user?.email}</span>
              <button onClick={handleLogout} className="text-slate-400 hover:text-red-400 text-sm font-medium transition-colors">
                Logout
              </button>
            </>
          ) : (
            <>
              <NavLink to="/login" className="text-slate-400 hover:text-white text-sm font-medium transition-colors">Login</NavLink>
              <NavLink to="/signup" className="bg-gradient-to-r from-brand-600 to-brand-500 hover:from-brand-500 hover:to-brand-400 text-white px-4 py-2 rounded-lg text-sm font-medium shadow-lg hover:shadow-xl transition-all duration-300">
                Sign up
              </NavLink>
            </>
          )}
        </nav>
      </div>
    </header>
  );
}
