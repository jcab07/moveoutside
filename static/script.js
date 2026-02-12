import { initializeApp } from "https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js";
import {
  getAuth,
  signInWithEmailAndPassword,
  signOut,
  onAuthStateChanged
} from "https://www.gstatic.com/firebasejs/10.7.1/firebase-auth.js";
import {
  getFirestore,
  collection,
  onSnapshot,
  addDoc,
  updateDoc,
  doc,
  query,
  where,
  orderBy,
  getDocs,
  getDoc,
  serverTimestamp
} from "https://www.gstatic.com/firebasejs/10.7.1/firebase-firestore.js";

/* ======================
   Firebase Config (TU PROYECTO)
   ====================== */
const firebaseConfig = {
  apiKey: "AIzaSyBDt4GsYfUN_tO6VkKo93IAzY7q1QLpbek",
  authDomain: "move-outside.firebaseapp.com",
  projectId: "move-outside",
  storageBucket: "move-outside.firebasestorage.app",
  messagingSenderId: "758659742167",
  appId: "1:758659742167:web:3bbf90b0c2b4f6d7c8f392",
  measurementId: "G-0Y0NHET0RB"
};

console.log("✅ script.js cargado");

/* ======================
   MODO AUTH
   - "firebase" (default): usa loginScreen y Firebase Auth
   - "flask": entra directo (sesión Flask) y solo usa Firestore
   ====================== */
const AUTH_MODE = (window.MOVE_AUTH_MODE || "firebase").toLowerCase();

/* Inicializa Firebase */
const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const db = getFirestore(app);

/* ======================
   Estado UI / Mapa
   ====================== */
let map = null;
let markers = {}; // { conductorId: L.marker }

/* Cache (para pintar listas sin pedir extra) */
let driversCache = {}; // {id: data}
let ordersCache = {};  // {id: data}

/* ======================
   Helpers DOM
   ====================== */
function $(id) {
  return document.getElementById(id);
}

function setText(id, text) {
  const el = $(id);
  if (el) el.textContent = text;
}

function setLoginError(msg) {
  const el = $("loginError");
  if (!el) return;
  el.textContent = msg || "";
}

function escapeHTML(s) {
  if (s === null || s === undefined) return "";
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

/* ======================
   Modal
   ====================== */
function openModal(id) {
  const modal = $(id);
  if (!modal) return;
  modal.style.display = "flex";
  modal.setAttribute("aria-hidden", "false");
}

function closeModal(id) {
  const modal = $(id);
  if (!modal) return;
  modal.style.display = "none";
  modal.setAttribute("aria-hidden", "true");
}

/* ======================
   Validación / Normalización
   ====================== */
function normalizePlate(raw) {
  const s = (raw || "").trim().toUpperCase().replace(/\s+/g, "");
  if (!s) return "";
  // Acepta 1234ABC o 1234-ABC
  const ok = /^\d{4}-?[A-Z]{3}$/.test(s);
  if (!ok) return null;
  // Normaliza a 1234-ABC
  return s.includes("-") ? s : (s.slice(0, 4) + "-" + s.slice(4));
}

function normalizeProject(raw) {
  const s = (raw || "").trim().toUpperCase().replace(/\s+/g, "");
  if (!s) return "";
  // Ejemplo: V429 (letra(s) + números)
  const ok = /^[A-Z]{1,5}\d{1,6}$/.test(s);
  if (!ok) return null;
  return s;
}

/* ======================
   AUTH (solo Firebase mode)
   ====================== */
async function login() {
  setLoginError("");

  const email = ($("email")?.value || "").trim();
  const password = $("password")?.value || "";

  if (!email || !password) {
    setLoginError("Introduce email y contraseña.");
    return;
  }

  try {
    await signInWithEmailAndPassword(auth, email, password);
  } catch (err) {
    console.error("Login error:", err);
    const code = err?.code || "";
    let msg = err?.message || "Error desconocido";

    if (code === "auth/invalid-credential" || code === "auth/wrong-password") msg = "Contraseña incorrecta.";
    else if (code === "auth/user-not-found") msg = "Ese usuario no existe en Firebase Authentication.";
    else if (code === "auth/invalid-email") msg = "Email inválido.";
    else if (code === "auth/network-request-failed") msg = "Error de red. Abre con Live Server y revisa tu conexión.";
    else if (code === "auth/operation-not-allowed") msg = "Email/Password no está habilitado en Firebase Authentication.";

    setLoginError(msg);
  }
}

async function logoutFirebase() {
  try {
    await signOut(auth);
  } catch (err) {
    console.error("Logout error:", err);
  }
}

/* ======================
   UI Binding (SIN onclick)
   ====================== */
function bindUI() {
  // Login (solo si modo firebase)
  if (AUTH_MODE === "firebase") {
    $("loginBtn")?.addEventListener("click", login);

    // Enter en password -> login
    $("password")?.addEventListener("keydown", (e) => {
      if (e.key === "Enter") login();
    });

    // Logout firebase (si existe botón)
    $("logoutBtn")?.addEventListener("click", logoutFirebase);
  } else {
    // En modo Flask, no usamos el botón logoutBtn (tu HTML ya tiene link /logout)
    // Si aún existe logoutBtn por compatibilidad, lo escondemos:
    const btn = $("logoutBtn");
    if (btn) btn.style.display = "none";
  }

  // Nuevo servicio
  $("newOrderBtn")?.addEventListener("click", () => openModal("orderModal"));

  // Modal: cancelar / confirmar
  $("orderCancelBtn")?.addEventListener("click", () => closeModal("orderModal"));
  $("orderConfirmBtn")?.addEventListener("click", createOrder);

  // Cerrar modal al click fuera
  const modal = $("orderModal");
  if (modal) {
    modal.addEventListener("click", (e) => {
      if (e.target === modal) closeModal("orderModal");
    });
  }

  // ESC para cerrar modal
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeModal("orderModal");
  });

  // Delegación: clicks en botones ASIGNAR dentro de ordersList
  $("ordersList")?.addEventListener("click", (e) => {
    const btn = e.target?.closest?.("button[data-action='assign']");
    if (!btn) return;
    const orderId = btn.getAttribute("data-order-id");
    if (orderId) assignOrder(orderId);
  });
}

document.addEventListener("DOMContentLoaded", () => {
  bindUI();

  // ✅ MODO FLASK: entra directo al panel
  if (AUTH_MODE === "flask") {
    const loginScreen = $("loginScreen");
    const mainScreen = $("mainScreen");

    if (loginScreen) loginScreen.style.display = "none";
    if (mainScreen) mainScreen.style.display = "flex";

    // Opcional: pedir usuario al backend Flask (/me)
    hydrateUserFromFlask().finally(() => {
      initDashboard();
    });
  }
});

/* ======================
   Flask: obtener usuario de sesión
   ====================== */
async function hydrateUserFromFlask() {
  try {
    const res = await fetch("/me", { credentials: "include" });
    if (!res.ok) return;
    const data = await res.json();
    if (data?.username) {
      setText("userEmail", data.username);
    }
  } catch (_) {
    // silencio: no rompe nada
  }
}

/* ======================
   Auth State (solo Firebase mode)
   ====================== */
if (AUTH_MODE === "firebase") {
  onAuthStateChanged(auth, (user) => {
    if (user) {
      $("loginScreen").style.display = "none";
      $("mainScreen").style.display = "flex";
      setText("userEmail", user.email || "");
      initDashboard();
    } else {
      $("loginScreen").style.display = "flex";
      $("mainScreen").style.display = "none";
    }
  });
}

/* ======================
   Dashboard init
   ====================== */
function initDashboard() {
  initMap();
  listenConductores();
  listenPedidos();
}

/* ======================
   MAPA
   ====================== */
function initMap() {
  if (map) return;

  // Valdemoro aprox (ajusta si quieres)
  map = L.map("map").setView([40.1919, -3.6806], 15);

  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution: "© OpenStreetMap contributors"
  }).addTo(map);
}

function getLatLon(ubicacion) {
  // Formato A: {lat, lon}
  if (ubicacion && typeof ubicacion.lat === "number" && typeof ubicacion.lon === "number") {
    return { lat: ubicacion.lat, lon: ubicacion.lon };
  }
  // Formato B: GeoPoint (Firestore)
  if (ubicacion && typeof ubicacion.latitude === "number" && typeof ubicacion.longitude === "number") {
    return { lat: ubicacion.latitude, lon: ubicacion.longitude };
  }
  return { lat: null, lon: null };
}

/* ======================
   Conductores (Firestore: conductores)
   ====================== */
function listenConductores() {
  const colRef = collection(db, "conductores");

  onSnapshot(colRef, (snapshot) => {
    const listEl = $("driversList");
    if (listEl) listEl.innerHTML = "";

    driversCache = {};
    let libres = 0;
    const seen = new Set();

    snapshot.forEach((d) => {
      const id = d.id;
      const data = d.data() || {};
      driversCache[id] = data;
      seen.add(id);

      if ((data.estado || "").toLowerCase() === "libre") libres++;

      // UI
      if (listEl) {
        const estado = (data.estado || "—").toLowerCase();
        const badgeClass =
          estado === "libre" ? "status-libre" :
          estado === "ocupado" ? "status-ocupado" :
          "status-otros";

        const matriculaMostrar = data.matricula_actual || data.matricula_tractora || "—";
        const proyectoMostrar = data.proyecto_actual || "—";

        const row = document.createElement("div");
        row.className = "item-row";
        row.innerHTML = `
          <div>
            <strong>${escapeHTML(data.nombre || "—")}</strong><br/>
            <small>Tractora: ${escapeHTML(data.matricula_tractora || "—")}</small><br/>
            <small>Matrícula actual: ${escapeHTML(matriculaMostrar)}</small><br/>
            <small>Proyecto: ${escapeHTML(proyectoMostrar)}</small>
          </div>
          <div style="text-align:right;">
            <span class="status-badge ${badgeClass}">${escapeHTML(data.estado || "—")}</span>
          </div>
        `;
        listEl.appendChild(row);
      }

      // Mapa
      const { lat, lon } = getLatLon(data.ubicacion);
      if (typeof lat === "number" && typeof lon === "number") {
        if (markers[id]) {
          markers[id].setLatLng([lat, lon]);
        } else {
          const marker = L.marker([lat, lon], { title: data.nombre || id }).addTo(map);
          marker.bindPopup(
            `<strong>${escapeHTML(data.nombre || id)}</strong><br/>
             <small>${escapeHTML(data.matricula_tractora || "")}</small>`
          );
          markers[id] = marker;
        }
      }
    });

    // Quita markers de conductores eliminados
    Object.keys(markers).forEach((markerId) => {
      if (!seen.has(markerId)) {
        try { map.removeLayer(markers[markerId]); } catch (_) {}
        delete markers[markerId];
      }
    });

    setText("count-drivers", String(libres));
  }, (err) => {
    console.error("Error escuchando conductores:", err);
  });
}

/* ======================
   Pedidos / Servicios (Firestore: pedidos)
   ====================== */
function listenPedidos() {
  const q = query(collection(db, "pedidos"), orderBy("fecha_creacion", "desc"));

  onSnapshot(q, (snapshot) => {
    const listEl = $("ordersList");
    if (listEl) listEl.innerHTML = "";

    ordersCache = {};
    let total = 0;
    let activos = 0;

    snapshot.forEach((d) => {
      total++;
      const p = d.data() || {};
      ordersCache[d.id] = p;

      if ((p.estado || "").toLowerCase() !== "finalizado") activos++;

      const estado = (p.estado || "—").toLowerCase();
      const plate = p.matricula || "—";
      const project = p.proyecto || "—";
      const shortId = d.id.slice(-6);

      const row = document.createElement("div");
      row.className = "item-row";
      row.innerHTML = `
        <div>
          <strong>Servicio #${escapeHTML(shortId)}</strong><br/>
          <small>${escapeHTML(p.origen || "—")} ➜ ${escapeHTML(p.destino || "—")}</small><br/>
          <small>Matrícula: ${escapeHTML(plate)} · Proyecto: ${escapeHTML(project)}</small><br/>
          <small>Estado: ${escapeHTML(p.estado || "—")}</small>
        </div>
        <div style="text-align:right;">
          ${
            estado === "pendiente"
              ? `<button class="btn-assign" data-action="assign" data-order-id="${escapeHTML(d.id)}">ASIGNAR</button>`
              : ""
          }
        </div>
      `;
      listEl.appendChild(row);
    });

    setText("count-total", String(total));
    setText("count-active", String(activos));
  }, (err) => {
    console.error("Error escuchando pedidos:", err);
  });
}

/* ======================
   Crear servicio
   ====================== */
async function createOrder() {
  const origin = ($("orderOrigin")?.value || "").trim();
  const dest = ($("orderDest")?.value || "").trim();
  const priority = $("orderPriority")?.value || "Normal";

  const plateNorm = normalizePlate($("orderPlate")?.value || "");
  const projectNorm = normalizeProject($("orderProject")?.value || "");

  if (!origin || !dest) {
    alert("Rellena origen y destino.");
    return;
  }
  if (plateNorm === null) {
    alert("Matrícula inválida. Ejemplo: 1234-ABC");
    return;
  }
  if (projectNorm === null) {
    alert("Proyecto inválido. Ejemplo: V429");
    return;
  }

  try {
    await addDoc(collection(db, "pedidos"), {
      origen: origin,
      destino: dest,
      prioridad: priority,
      matricula: plateNorm || "",
      proyecto: projectNorm || "",
      estado: "pendiente",
      id_conductor: null,
      fecha_creacion: serverTimestamp()
    });

    // Limpia inputs y cierra
    $("orderOrigin").value = "";
    $("orderDest").value = "";
    $("orderPlate").value = "";
    $("orderProject").value = "";
    $("orderPriority").value = "Normal";

    closeModal("orderModal");
  } catch (err) {
    console.error("Error creando pedido:", err);
    alert("No se pudo crear el servicio: " + (err?.message || err));
  }
}

/* ======================
   Asignar servicio
   ====================== */
async function assignOrder(orderId) {
  try {
    // 1) Buscar pedido para obtener matrícula y proyecto
    const orderRef = doc(db, "pedidos", orderId);
    const orderSnap = await getDoc(orderRef);
    if (!orderSnap.exists()) {
      alert("No encuentro ese servicio.");
      return;
    }
    const orderData = orderSnap.data() || {};
    const plate = orderData.matricula || "";
    const project = orderData.proyecto || "";

    // 2) Buscar 1 conductor libre
    const qDrivers = query(collection(db, "conductores"), where("estado", "==", "libre"));
    const snapDrivers = await getDocs(qDrivers);

    if (snapDrivers.empty) {
      alert("No hay conductores libres ahora mismo.");
      return;
    }

    // (simple) cogemos el primero
    const driverDoc = snapDrivers.docs[0];
    const driverId = driverDoc.id;

    // 3) Actualizar pedido
    await updateDoc(orderRef, {
      id_conductor: driverId,
      estado: "asignado",
      fecha_asignacion: serverTimestamp()
    });

    // 4) Actualizar conductor
    const driverRef = doc(db, "conductores", driverId);
    await updateDoc(driverRef, {
      estado: "ocupado",
      matricula_actual: plate,
      proyecto_actual: project,
      servicio_activo_id: orderId,
      ultimo_update: serverTimestamp()
    });

    alert(`Asignado a ${driverDoc.data()?.nombre || driverId}`);
  } catch (err) {
    console.error("Error asignando servicio:", err);
    alert("Error al asignar: " + (err?.message || err));
  }
}
