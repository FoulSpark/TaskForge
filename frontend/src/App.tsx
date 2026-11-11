import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import { AuthProvider, useAuth } from "./auth/AuthContext";
import { ShellLayout } from "./components/ShellLayout";
import { MonitoringPage } from "./pages/MonitoringPage";
import { OverviewPage } from "./pages/OverviewPage";
import { SignInPage } from "./pages/SignInPage";
import { SubmitTaskPage } from "./pages/SubmitTaskPage";
import { TaskDetailPage } from "./pages/TaskDetailPage";
import { TaskExplorerPage } from "./pages/TaskExplorerPage";

export function AuthenticatedRoutes() {
  const { isAuthenticated } = useAuth();

  if (!isAuthenticated) {
    return <SignInPage />;
  }

  return (
    <Routes>
      <Route element={<ShellLayout />}>
        <Route path="/" element={<OverviewPage />} />
        <Route path="/ui" element={<Navigate to="/" replace />} />
        <Route path="/submit" element={<SubmitTaskPage />} />
        <Route path="/tasks" element={<TaskExplorerPage />} />
        <Route path="/tasks/:taskId" element={<TaskDetailPage />} />
        <Route path="/monitoring" element={<MonitoringPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Route>
    </Routes>
  );
}

export function App() {
  return (
    <AuthProvider>
      <BrowserRouter>
        <AuthenticatedRoutes />
      </BrowserRouter>
    </AuthProvider>
  );
}

