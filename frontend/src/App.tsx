import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";

import Layout from "./components/Layout";
import Dashboard from "./views/Dashboard";
import ChatsView from "./views/ChatsView";
import MemoriesView from "./views/MemoriesView";

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route index element={<Dashboard />} />
          <Route path="/dashboard" element={<Navigate replace to="/" />} />
          <Route path="/memories" element={<MemoriesView />} />
          <Route path="/chats" element={<ChatsView />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}
