import { Routes, Route, Navigate } from 'react-router-dom';
import { AuthProvider } from './context/AuthContext.jsx';
import Layout from './components/Layout.jsx';
import PrivateRoute from './components/PrivateRoute.jsx';
import Home from './pages/Home.jsx';
import Login from './pages/Login.jsx';
import Signup from './pages/Signup.jsx';
import Dashboard from './pages/Dashboard.jsx';
import WatchList from './pages/WatchList.jsx';
import StockDetail from './pages/StockDetail.jsx';
import Chat from './pages/Chat.jsx';
import SentimentAnalysis from './pages/SentimentAnalysis.jsx';
import Simulator from './pages/Simulator.jsx';

function AppRoutes() {
  return (
    <Routes>
      <Route path="/" element={<Layout />}>
        <Route index element={<PrivateRoute><Home /></PrivateRoute>} />
        <Route path="login" element={<Login />} />
        <Route path="signup" element={<Signup />} />
    <Route path="dashboard" element={<PrivateRoute><Dashboard /></PrivateRoute>} />
    <Route path="dashboard/:symbol" element={<PrivateRoute><Dashboard /></PrivateRoute>} />
    <Route path="watch-list" element={<PrivateRoute><WatchList /></PrivateRoute>} />
        <Route path="stock/:symbol" element={<PrivateRoute><StockDetail /></PrivateRoute>} />
        <Route path="chat" element={<PrivateRoute><Chat /></PrivateRoute>} />
    <Route path="simulator" element={<PrivateRoute><Simulator /></PrivateRoute>} />
        <Route path="sentiment-analysis" element={<PrivateRoute><SentimentAnalysis /></PrivateRoute>} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Route>
    </Routes>
  );
}

export default function App() {
  return (
    <AuthProvider>
      <AppRoutes />
    </AuthProvider>
  );
}
