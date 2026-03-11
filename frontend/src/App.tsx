import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";

import Layout from "./components/Layout";
import Dashboard from "./views/Dashboard";
import Chats from "./views/Chats";
import Memories from "./views/Memories";

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route index element={<Dashboard />} />
          <Route path="/dashboard" element={<Navigate replace to="/" />} />
          <Route path="/memories" element={<Memories />} />
          <Route path="/chats" element={<Chats />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}
