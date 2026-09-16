// HYPERFLIX: Lógica de Interfaz, HLS IPTV Player y Emisor de Telemetría en Vivo

// 1. Catálogo de Canales IPTV Públicos y Libres (Streams HLS .m3u8 reales)
const CANALES_IPTV = [
  {
    id: "IPTV-001",
    nombre: "France 24 Español",
    categoria: "Noticias Internacionales",
    pais: "Francia / Global",
    url: "https://f24hls-i.akamaihd.net/hls/live/221193/F24_ES_LO_HLS/master_500.m3u8",
    logo: "https://upload.wikimedia.org/wikipedia/commons/4/41/France24_2013.svg",
    bitrate: "4.8 Mbps",
    resolucion: "1080p HD"
  },
  {
    id: "IPTV-002",
    nombre: "DW Español (Deutsche Welle)",
    categoria: "Noticias & Documentales",
    pais: "Alemania",
    url: "https://dwamdstream104.akamaized.net/hls/live/2015530/dwstream104/index.m3u8",
    logo: "https://upload.wikimedia.org/wikipedia/commons/7/75/Deutsche_Welle_logo.svg",
    bitrate: "5.5 Mbps",
    resolucion: "1080p HD"
  },
  {
    id: "IPTV-003",
    nombre: "RTVE 24h España",
    categoria: "Noticias 24/7",
    pais: "España",
    url: "https://ztnr.rtve.es/ztnr/1694255.m3u8",
    logo: "https://upload.wikimedia.org/wikipedia/commons/e/e4/Canal_24_horas_2008.svg",
    bitrate: "3.2 Mbps",
    resolucion: "720p HD"
  },
  {
    id: "IPTV-004",
    nombre: "NASA TV Public HD",
    categoria: "Ciencia & Espacio",
    pais: "Estados Unidos",
    url: "https://ntv1.akamaized.net/hls/live/2014075/NASA-NTV1-HLS/master.m3u8",
    logo: "https://upload.wikimedia.org/wikipedia/commons/e/e5/NASA_logo.svg",
    bitrate: "6.0 Mbps",
    resolucion: "1080p 60fps"
  },
  {
    id: "IPTV-005",
    nombre: "Red Bull TV",
    categoria: "Deportes Extremos",
    pais: "Global",
    url: "https://rbmn-live.akamaized.net/hls/live/590964/BoRB-AT/master.m3u8",
    logo: "https://upload.wikimedia.org/wikipedia/commons/7/7b/Red_Bull_logo.svg",
    bitrate: "5.8 Mbps",
    resolucion: "1080p HD"
  }
];

// 2. Catálogo Demostrativo de Películas VOD
const PELICULAS_VOD = [
  {
    id: "MOV-001",
    titulo: "Inception",
    genero: "Ciencia Ficción",
    duracion: "148 min",
    anio: 2010,
    calidad: "4K UHD",
    rating: "8.8",
    imagen: "https://images.unsplash.com/photo-1536440136628-849c177e76a1?w=500&auto=format&fit=crop"
  },
  {
    id: "MOV-002",
    titulo: "Interstellar",
    genero: "Ciencia Ficción",
    duracion: "169 min",
    anio: 2014,
    calidad: "4K UHD",
    rating: "8.7",
    imagen: "https://images.unsplash.com/photo-1451187580459-43490279c0fa?w=500&auto=format&fit=crop"
  },
  {
    id: "MOV-003",
    titulo: "The Dark Knight",
    genero: "Acción",
    duracion: "152 min",
    anio: 2008,
    calidad: "4K UHD",
    rating: "9.0",
    imagen: "https://images.unsplash.com/photo-1509198397868-475647b2a1e5?w=500&auto=format&fit=crop"
  },
  {
    id: "MOV-004",
    titulo: "Pulp Fiction",
    genero: "Crimen",
    duracion: "154 min",
    anio: 1994,
    calidad: "1080p HD",
    rating: "8.9",
    imagen: "https://images.unsplash.com/photo-1594909122845-11baa439b7bf?w=500&auto=format&fit=crop"
  },
  {
    id: "MOV-005",
    titulo: "Parasite",
    genero: "Drama",
    duracion: "132 min",
    anio: 2019,
    calidad: "4K UHD",
    rating: "8.5",
    imagen: "https://images.unsplash.com/photo-1489599849927-2ee91cede3ba?w=500&auto=format&fit=crop"
  },
  {
    id: "MOV-006",
    titulo: "Cyberpunk 2099",
    genero: "Ciencia Ficción",
    duracion: "115 min",
    anio: 2024,
    calidad: "4K UHD",
    rating: "8.3",
    imagen: "https://images.unsplash.com/photo-1578632767115-351597cf2477?w=500&auto=format&fit=crop"
  }
];

let hlsInstance = null;
let currentChannelIndex = 0;
let totalTelemetryEvents = 20003;

// Inicialización del DOM
document.addEventListener("DOMContentLoaded", () => {
  renderizarCanales();
  renderizarPeliculas(PELICULAS_VOD);
  configurarFiltrosGeneros();
  iniciarReproductorIPTV(CANALES_IPTV[0]);
  iniciarGeneradorTelemetriaFondo();
  configurarEventosUI();
});

// Renderizar la lista de Canales IPTV en la Guía
function renderizarCanales() {
  const container = document.getElementById("channels-list");
  container.innerHTML = "";

  CANALES_IPTV.forEach((canal, index) => {
    const item = document.createElement("button");
    item.className = `w-full text-left p-3 rounded-lg border transition flex items-center gap-3 ${
      index === currentChannelIndex
        ? "bg-brand-red/20 border-brand-red text-white"
        : "bg-[#181818] border-white/5 text-gray-300 hover:bg-white/5"
    }`;
    item.onclick = () => cambiarCanal(index);

    item.innerHTML = `
      <div class="w-10 h-10 rounded bg-white/10 p-1 flex items-center justify-center shrink-0">
        <i class="fa-solid fa-tv text-brand-red text-base"></i>
      </div>
      <div class="flex-1 min-w-0">
        <div class="flex items-center justify-between">
          <h5 class="text-xs font-bold text-white truncate">${canal.nombre}</h5>
          <span class="text-[10px] text-gray-400 font-mono">${canal.resolucion}</span>
        </div>
        <p class="text-[11px] text-gray-400 truncate">${canal.categoria}</p>
      </div>
    `;
    container.appendChild(item);
  });
}

// Inicializar reproductor HLS para streams IPTV
function iniciarReproductorIPTV(canal) {
  const video = document.getElementById("iptv-video");
  const titleEl = document.getElementById("current-channel-title");
  const descEl = document.getElementById("current-channel-desc");
  const bitrateEl = document.getElementById("current-bitrate");
  const overlay = document.getElementById("video-overlay");
  const overlayText = document.getElementById("overlay-text");

  titleEl.textContent = canal.nombre;
  descEl.textContent = `${canal.categoria} • ${canal.pais}`;
  bitrateEl.textContent = canal.bitrate;

  overlay.classList.remove("opacity-0");
  overlayText.textContent = `Sintonizando ${canal.nombre}...`;

  // Destruir instancia anterior si existe
  if (hlsInstance) {
    hlsInstance.destroy();
  }

  // Comprobar soporte de HLS.js
  if (Hls.isSupported()) {
    hlsInstance = new Hls({
      enableWorker: true,
      lowLatencyMode: true,
      backBufferLength: 60
    });
    hlsInstance.loadSource(canal.url);
    hlsInstance.attachMedia(video);

    hlsInstance.on(Hls.Events.MANIFEST_PARSED, () => {
      overlay.classList.add("opacity-0");
      video.play().catch(() => {
        // Autoplay policy: si falla el sonido, reproducir muteado
        video.muted = true;
        video.play();
      });
      despacharEventoTelemetria("STREAM_START", canal.nombre, "Canal_TV", canal.bitrate, "32ms");
    });

    hlsInstance.on(Hls.Events.ERROR, (event, data) => {
      if (data.fatal) {
        switch (data.type) {
          case Hls.ErrorTypes.NETWORK_ERROR:
            overlayText.textContent = "Reconectando stream HLS...";
            hlsInstance.startLoad();
            despacharEventoTelemetria("BUFFERING_RETRY", canal.nombre, "Canal_TV", "0 Mbps", "180ms");
            break;
          case Hls.ErrorTypes.MEDIA_ERROR:
            hlsInstance.recoverMediaError();
            break;
          default:
            hlsInstance.destroy();
            overlayText.textContent = "Stream público en mantenimiento.";
            break;
        }
      }
    });

  } else if (video.canPlayType("application/vnd.apple.mpegurl")) {
    // Soporte nativo para Safari iOS / macOS
    video.src = canal.url;
    video.addEventListener("loadedmetadata", () => {
      overlay.classList.add("opacity-0");
      video.play();
      despacharEventoTelemetria("STREAM_START", canal.nombre, "Canal_TV", canal.bitrate, "28ms");
    });
  } else {
    overlayText.textContent = "El navegador no soporta streaming HLS.";
  }
}

// Cambiar de canal IPTV
function cambiarCanal(index) {
  currentChannelIndex = index;
  renderizarCanales();
  const canal = CANALES_IPTV[index];
  iniciarReproductorIPTV(canal);
  despacharEventoTelemetria("CHANNEL_SWITCH", canal.nombre, "Canal_TV", canal.bitrate, "45ms");
}

// Renderizar Cuadrícula de Películas VOD
function renderizarPeliculas(lista) {
  const container = document.getElementById("movies-grid");
  container.innerHTML = "";

  lista.forEach(pelicula => {
    const card = document.createElement("div");
    card.className = "bg-[#202020] rounded-xl overflow-hidden border border-white/10 group hover:border-brand-red transition duration-300 flex flex-col";
    
    card.innerHTML = `
      <div class="relative aspect-[2/3] overflow-hidden bg-black">
        <img src="${pelicula.imagen}" alt="${pelicula.titulo}" class="w-full h-full object-cover group-hover:scale-105 transition duration-500">
        <div class="absolute inset-0 bg-gradient-to-t from-black/80 via-transparent to-black/20"></div>
        <span class="absolute top-2 right-2 px-1.5 py-0.5 rounded text-[10px] font-bold bg-brand-red text-white uppercase">${pelicula.calidad}</span>
        <span class="absolute bottom-2 left-2 px-1.5 py-0.5 rounded text-[10px] font-bold bg-black/70 text-yellow-400 border border-yellow-500/30">
          <i class="fa-solid fa-star text-[9px] mr-1"></i>${pelicula.rating}
        </span>
      </div>
      <div class="p-3 flex-1 flex flex-col justify-between">
        <div>
          <h4 class="text-sm font-bold text-white group-hover:text-brand-red transition truncate">${pelicula.titulo}</h4>
          <div class="flex items-center justify-between text-[11px] text-gray-400 mt-1">
            <span>${pelicula.genero}</span>
            <span>${pelicula.anio}</span>
          </div>
        </div>
        <button class="mt-3 w-full bg-white/10 hover:bg-brand-red text-white text-xs font-bold py-1.5 rounded transition flex items-center justify-center gap-1 btn-ver-pelicula" data-id="${pelicula.id}">
          <i class="fa-solid fa-play text-[10px]"></i> Ver Ahora
        </button>
      </div>
    `;
    container.appendChild(card);
  });

  // Asignar eventos de click a botones de película
  document.querySelectorAll(".btn-ver-pelicula").forEach(btn => {
    btn.onclick = () => {
      const peliId = btn.getAttribute("data-id");
      const peli = PELICULAS_VOD.find(p => p.id === peliId);
      abrirModalPelicula(peli);
    };
  });
}

// Configurar Filtros por Género
function configurarFiltrosGeneros() {
  const buttons = document.querySelectorAll(".genre-btn");
  buttons.forEach(btn => {
    btn.onclick = () => {
      buttons.forEach(b => {
        b.classList.remove("bg-brand-red", "text-white", "active");
        b.classList.add("bg-[#282828]", "text-gray-300");
      });
      btn.classList.add("bg-brand-red", "text-white", "active");
      btn.classList.remove("bg-[#282828]", "text-gray-300");

      const genero = btn.getAttribute("data-genre");
      if (genero === "todos") {
        renderizarPeliculas(PELICULAS_VOD);
      } else {
        const filtradas = PELICULAS_VOD.filter(p => p.genero === genero);
        renderizarPeliculas(filtradas);
      }
      despacharEventoTelemetria("FILTER_CATALOG", genero, "Filtro", "0 Mbps", "12ms");
    };
  });
}

// Modal de Película VOD
function abrirModalPelicula(pelicula) {
  const modal = document.getElementById("movie-modal");
  document.getElementById("modal-movie-title").textContent = `${pelicula.titulo} (${pelicula.anio})`;
  document.getElementById("modal-movie-name").textContent = pelicula.titulo;
  document.getElementById("modal-movie-meta").textContent = `${pelicula.genero} • ${pelicula.duracion} • ${pelicula.calidad}`;
  document.getElementById("modal-movie-bg").style.backgroundImage = `url('${pelicula.imagen}')`;
  
  modal.classList.remove("hidden");
  modal.classList.add("flex");
  
  despacharEventoTelemetria("MOVIE_PAGE_VIEW", pelicula.titulo, "Pelicula", "15.0 Mbps", "24ms");
}

function cerrarModalPelicula() {
  const modal = document.getElementById("movie-modal");
  modal.classList.add("hidden");
  modal.classList.remove("flex");
}

// Despachar Evento a la Consola de Telemetría (Simulación Kafka)
function despacharEventoTelemetria(tipo, contenido, categoria, bitrate, latencia) {
  totalTelemetryEvents++;
  document.getElementById("total-telemetry-count").textContent = totalTelemetryEvents.toLocaleString();

  const terminal = document.getElementById("telemetry-terminal");
  const line = document.createElement("div");
  line.className = "flex items-start gap-2 py-0.5 border-b border-white/5 font-mono text-[11px]";

  const now = new Date().toLocaleTimeString();
  const userId = `USR-${Math.floor(10000 + Math.random() * 90000)}`;
  const dispositivos = ["Smart TV Samsung (Tizen)", "Xiaomi Redmi (Android 14)", "iPhone 15 (iOS 17)", "PC Chrome (Win11)"];
  const disp = dispositivos[Math.floor(Math.random() * dispositivos.length)];

  let colorBadge = "text-blue-400";
  if (tipo.includes("START") || tipo.includes("PLAY")) colorBadge = "text-emerald-400";
  if (tipo.includes("BUFFER") || tipo.includes("RETRY")) colorBadge = "text-yellow-400";
  if (tipo.includes("SWITCH")) colorBadge = "text-purple-400";

  line.innerHTML = `
    <span class="text-gray-500 shrink-0">[${now}]</span>
    <span class="font-bold ${colorBadge} shrink-0">[${tipo}]</span>
    <span class="text-gray-400 truncate">usr: <strong class="text-gray-200">${userId}</strong> | item: <strong class="text-white">${contenido}</strong> | disp: <em>${disp}</em> | bit: <span class="text-emerald-300">${bitrate}</span> | lat: ${latencia} ➔ <span class="text-orange-400">Kafka::hyperflix.telemetry</span></span>
  `;

  terminal.prepend(line);

  // Mantener máximo 50 líneas en pantalla
  if (terminal.children.length > 50) {
    terminal.removeChild(terminal.lastChild);
  }
}

// Generador de eventos de telemetría de fondo (simula usuarios concurrentes)
function iniciarGeneradorTelemetriaFondo() {
  const eventosPosibles = [
    { tipo: "HEARTBEAT_10S", cont: "DW Español", cat: "Canal_TV", bit: "5.2 Mbps", lat: "35ms" },
    { tipo: "PLAY_SEEK", cont: "Inception", cat: "Pelicula", bit: "18.2 Mbps", lat: "28ms" },
    { tipo: "HEARTBEAT_10S", cont: "France 24 Español", cat: "Canal_TV", bit: "4.8 Mbps", lat: "42ms" },
    { tipo: "QUALITY_ADAPT", cont: "Interstellar", cat: "Pelicula", bit: "12.5 Mbps", lat: "65ms" },
    { tipo: "HEARTBEAT_10S", cont: "NASA TV Public HD", cat: "Canal_TV", bit: "6.1 Mbps", lat: "31ms" }
  ];

  setInterval(() => {
    const item = eventosPosibles[Math.floor(Math.random() * eventosPosibles.length)];
    despacharEventoTelemetria(item.tipo, item.cont, item.cat, item.bit, item.lat);
  }, 4000);
}

// Configuración de Eventos UI
function configurarEventosUI() {
  document.getElementById("close-modal").onclick = cerrarModalPelicula;
  document.getElementById("movie-modal").onclick = (e) => {
    if (e.target.id === "movie-modal") cerrarModalPelicula();
  };

  document.getElementById("btn-play-sim").onclick = () => {
    const title = document.getElementById("modal-movie-name").textContent;
    despacharEventoTelemetria("PLAY_START_VOD", title, "Pelicula", "19.5 Mbps (4K)", "21ms");
    alert(`▶️ Reproduciendo '${title}' en streaming seguro vía URL firmada de MinIO S3.`);
    cerrarModalPelicula();
  };

  document.getElementById("btn-simular-evento").onclick = () => {
    for (let i = 0; i < 5; i++) {
      setTimeout(() => {
        const acciones = ["PLAY_BURST", "PAUSE_BUFFER", "SEEK_FORWARD", "QUALITY_UPGRADE", "COMPLETE_SESSION"];
        const act = acciones[i];
        despacharEventoTelemetria(act, "Ráfaga Concurrente #" + (i + 1), "Stress Test", "14.2 Mbps", `${20 + i * 8}ms`);
      }, i * 300);
    }
  };
}
