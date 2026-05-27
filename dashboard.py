import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time

st.set_page_config(
    page_title="Spotify Stream Analytics",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;700;800&family=DM+Mono:wght@300;400;500&display=swap');

  html, body, [class*="css"] {
    font-family: 'DM Mono', monospace;
    background-color: #0a0a0f;
    color: #e8e8f0;
  }

  .main { background-color: #0a0a0f; }
  .block-container { padding: 2rem 2rem 2rem 2rem; }

  h1, h2, h3 {
    font-family: 'Syne', sans-serif !important;
    font-weight: 800 !important;
    letter-spacing: -0.02em;
  }

  .stMetric {
    background: linear-gradient(135deg, #111118 0%, #1a1a2e 100%);
    border: 1px solid #1db95433;
    border-radius: 12px;
    padding: 1rem 1.2rem;
  }

  .stMetric label {
    color: #1db954 !important;
    font-family: 'DM Mono', monospace !important;
    font-size: 0.7rem !important;
    text-transform: uppercase;
    letter-spacing: 0.1em;
  }

  .stMetric [data-testid="stMetricValue"] {
    font-family: 'Syne', sans-serif !important;
    font-size: 2rem !important;
    font-weight: 800 !important;
    color: #ffffff !important;
  }

  section[data-testid="stSidebar"] {
    background: #0d0d18;
    border-right: 1px solid #1db95422;
  }

  section[data-testid="stSidebar"] .stSelectbox label,
  section[data-testid="stSidebar"] .stSlider label,
  section[data-testid="stSidebar"] .stNumberInput label {
    color: #1db954 !important;
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
  }

  .stTabs [data-baseweb="tab-list"] {
    background: #111118;
    border-radius: 10px;
    padding: 4px;
    gap: 4px;
  }

  .stTabs [data-baseweb="tab"] {
    font-family: 'DM Mono', monospace;
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #888;
    border-radius: 8px;
    padding: 0.4rem 1.2rem;
  }

  .stTabs [aria-selected="true"] {
    background: #1db954 !important;
    color: #000 !important;
    font-weight: 500;
  }

  .stDataFrame { border: 1px solid #1db95422; border-radius: 8px; }

  div[data-testid="stVerticalBlock"] > div:has(.section-title) {
    margin-top: 1.5rem;
  }

  .section-title {
    font-family: 'Syne', sans-serif;
    font-size: 1.1rem;
    font-weight: 700;
    color: #1db954;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    border-bottom: 1px solid #1db95433;
    padding-bottom: 0.4rem;
    margin-bottom: 1rem;
  }

  .refresh-badge {
    display: inline-block;
    background: #1db95422;
    color: #1db954;
    border: 1px solid #1db95444;
    border-radius: 20px;
    padding: 2px 12px;
    font-size: 0.7rem;
    font-family: 'DM Mono', monospace;
    letter-spacing: 0.05em;
  }
</style>
""", unsafe_allow_html=True)

# ─── PLOTLY THEME ─────────────────────────────────────────────────────────────
PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="DM Mono, monospace", color="#e8e8f0", size=11),
    colorway=["#1db954", "#1ed760", "#158a3e", "#0f5f2b", "#a8e6c0",
              "#17a349", "#23d45e", "#52e07c", "#7eeba0", "#b0f0c8"],
    margin=dict(l=20, r=20, t=40, b=20),
)

DARK_AXIS = dict(gridcolor="#1a1a2e", linecolor="#1a1a2e", tickfont=dict(size=10))

# ─── CONNECTION ───────────────────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    return duckdb.connect("spotify_stream.duckdb", read_only=True)

con = get_connection()

# ─── DATA LOADERS ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=30)
def load_kpis():
    return con.execute("""
        SELECT
            (SELECT SUM(occurrences)              FROM gold_top_tracks)   AS total_tracks,
            (SELECT COUNT(*)                      FROM gold_playlist_stats) AS total_playlists,
            (SELECT COUNT(*)                      FROM gold_top_artists)  AS total_artists,
            (SELECT COUNT(DISTINCT album_name)    FROM gold_top_tracks)   AS total_albums
    """).df()

@st.cache_data(ttl=30)
def load_top_artists(limit=20):
    return con.execute(f"""
        SELECT artist_name,
               nb_playlists,
               nb_track_appearances AS nb_tracks,
               avg_duration_min
        FROM gold_top_artists
        ORDER BY nb_playlists DESC
        LIMIT {limit}
    """).df()

@st.cache_data(ttl=30)
def load_top_tracks(limit=20):
    return con.execute(f"""
        SELECT track_name, artist_name, occurrences, nb_playlists
        FROM gold_top_tracks
        ORDER BY occurrences DESC
        LIMIT {limit}
    """).df()

@st.cache_data(ttl=30)
def load_playlists_overview(min_followers=0, limit=100):
    return con.execute(f"""
        SELECT playlist_name, num_tracks, num_albums, num_followers,
               album_diversity_ratio AS album_diversity
        FROM gold_playlist_stats
        WHERE num_followers >= {min_followers}
        ORDER BY num_followers DESC
        LIMIT {limit}
    """).df()

@st.cache_data(ttl=30)
def load_followers_vs_tracks(sample=5000):
    return con.execute(f"""
        SELECT num_tracks, num_albums, num_followers, playlist_name
        FROM gold_playlist_stats
        WHERE num_followers > 0 AND num_tracks > 0
        USING SAMPLE {sample}
    """).df()

@st.cache_data(ttl=30)
def load_duration_distribution():
    return con.execute("""
        SELECT bucket_sec, bucket_min, nb_tracks AS count
        FROM gold_duration_analysis
        ORDER BY bucket_sec
    """).df()

@st.cache_data(ttl=30)
def load_duration_by_artist(min_tracks=15, limit=25):
    return con.execute(f"""
        SELECT artist_name,
               avg_duration_min AS avg_min,
               min_duration_min AS min_min,
               max_duration_min AS max_min,
               nb_track_appearances AS nb_tracks
        FROM gold_top_artists
        WHERE nb_track_appearances >= {min_tracks}
        ORDER BY avg_duration_min DESC
        LIMIT {limit}
    """).df()

@st.cache_data(ttl=30)
def load_position_analysis():
    return con.execute("""
        SELECT position, unique_tracks, unique_artists,
               avg_duration_min AS avg_duration
        FROM gold_position_patterns
        ORDER BY position
    """).df()

@st.cache_data(ttl=30)
def load_artist_filter():
    return con.execute("""
        SELECT artist_name FROM gold_top_artists
        ORDER BY artist_name
    """).df()["artist_name"].tolist()

# ─── SIDEBAR ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🎵 Spotify Analytics")
    st.markdown('<span class="refresh-badge">⟳ ttl 30s</span>', unsafe_allow_html=True)
    st.markdown("---")

    st.markdown("### Filtres globaux")
    top_n = st.slider("Top N artistes / tracks", 5, 50, 20)
    min_followers = st.number_input("Followers min (playlists)", 0, 1000000, 0, step=1000)
    min_tracks_artist = st.slider("Tracks min par artiste", 5, 50, 15)

    st.markdown("---")
    auto_refresh = st.toggle("Auto-refresh (30s)", value=False)
    if auto_refresh:
        st.info("🔄 Rafraîchissement actif")

    st.markdown("---")
    st.caption("dbt Gold · DuckDB · Streamlit")
    st.caption("spotify_stream.duckdb")

# ─── HEADER ───────────────────────────────────────────────────────────────────
st.markdown("""
<h1 style='font-family:Syne,sans-serif;font-weight:800;font-size:2.4rem;
           background:linear-gradient(90deg,#1db954,#52e07c);
           -webkit-background-clip:text;-webkit-text-fill-color:transparent;
           margin-bottom:0.2rem;'>
  SPOTIFY STREAM ANALYTICS
</h1>
<p style='color:#555;font-size:0.75rem;font-family:DM Mono,monospace;
          letter-spacing:0.1em;margin-top:0;'>
  MILLION PLAYLIST DATASET · DUCKDB + STREAMLIT
</p>
""", unsafe_allow_html=True)

# ─── KPIs ─────────────────────────────────────────────────────────────────────
kpis = load_kpis()
k1, k2, k3, k4 = st.columns(4)
k1.metric("🎵 Total Tracks",     f"{kpis['total_tracks'][0]:,}")
k2.metric("📋 Playlists",         f"{kpis['total_playlists'][0]:,}")
k3.metric("🎤 Artistes uniques",  f"{kpis['total_artists'][0]:,}")
k4.metric("💿 Albums uniques",    f"{kpis['total_albums'][0]:,}")

st.markdown("<br>", unsafe_allow_html=True)

# ─── TABS ─────────────────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["🎤  Top Artistes & Tracks", "📋  Playlists", "⏱  Durées & Patterns"])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — TOP ARTISTES & TRACKS
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    col_a, col_b = st.columns(2, gap="large")

    with col_a:
        st.markdown('<div class="section-title">Top Artistes par Playlists</div>', unsafe_allow_html=True)
        df_artists = load_top_artists(top_n)
        fig = px.bar(
            df_artists, x="nb_playlists", y="artist_name",
            orientation="h", text="nb_playlists",
            labels={"nb_playlists": "Nb Playlists", "artist_name": ""},
            color="nb_playlists",
            color_continuous_scale=["#0f5f2b", "#1db954", "#52e07c"],
        )
        fig.update_traces(textposition="outside", textfont_size=9)
        fig.update_layout(**PLOTLY_LAYOUT, height=520,
                          xaxis=dict(**DARK_AXIS),
                          yaxis=dict(**DARK_AXIS, autorange="reversed"),
                          coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.markdown('<div class="section-title">Top Artistes — Durée Moyenne</div>', unsafe_allow_html=True)
        df_dur = load_duration_by_artist(min_tracks_artist, top_n)
        fig2 = px.scatter(
            df_dur, x="avg_min", y="artist_name",
            size="nb_tracks", color="avg_min",
            labels={"avg_min": "Durée moy. (min)", "artist_name": "", "nb_tracks": "Nb tracks"},
            color_continuous_scale=["#158a3e", "#1db954", "#7eeba0"],
            size_max=30,
        )
        fig2.update_layout(**PLOTLY_LAYOUT, height=520,
                           xaxis=dict(**DARK_AXIS),
                           yaxis=dict(**DARK_AXIS, autorange="reversed"),
                           coloraxis_showscale=False)
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown('<div class="section-title">Top Tracks les plus récurrents</div>', unsafe_allow_html=True)
    df_tracks = load_top_tracks(top_n)
    st.dataframe(
        df_tracks.style.background_gradient(subset=["occurrences", "nb_playlists"],
                                            cmap="Greens"),
        use_container_width=True, hide_index=True, height=380,
    )

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — PLAYLISTS
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    df_pl = load_playlists_overview(min_followers)
    df_scatter = load_followers_vs_tracks()

    col_c, col_d = st.columns(2, gap="large")

    with col_c:
        st.markdown('<div class="section-title">Followers vs Nombre de Tracks</div>', unsafe_allow_html=True)
        fig3 = go.Figure()
        fig3.add_trace(go.Scatter(
            x=df_scatter["num_tracks"],
            y=df_scatter["num_followers"],
            mode="markers",
            marker=dict(
                color="#1db954",
                size=4,
                opacity=0.5,
            ),
            text=df_scatter["playlist_name"],
            hovertemplate="<b>%{text}</b><br>Tracks: %{x}<br>Followers: %{y}<extra></extra>",
        ))
        fig3.update_layout(
            **PLOTLY_LAYOUT, height=420,
            xaxis=dict(**DARK_AXIS, title="Nb Tracks"),
            yaxis=dict(**DARK_AXIS, title="Followers", type="log"),
        )
        st.plotly_chart(fig3, use_container_width=True)
    with col_d:
        st.markdown('<div class="section-title">Distribution — Taille des Playlists</div>', unsafe_allow_html=True)
        fig4 = px.histogram(
            df_scatter, x="num_tracks", nbins=50,
            labels={"num_tracks": "Nb Tracks", "count": "Nb Playlists"},
            color_discrete_sequence=["#1db954"],
        )
        fig4.update_layout(**PLOTLY_LAYOUT, height=420,
                           xaxis=dict(**DARK_AXIS),
                           yaxis=dict(**DARK_AXIS),
                           bargap=0.05)
        st.plotly_chart(fig4, use_container_width=True)

    st.markdown('<div class="section-title">Top Playlists par Followers</div>', unsafe_allow_html=True)
    st.dataframe(
        df_pl.style.background_gradient(subset=["num_followers", "num_tracks"],
                                        cmap="Greens"),
        use_container_width=True, hide_index=True, height=380,
    )

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — DURÉES & PATTERNS
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    col_e, col_f = st.columns(2, gap="large")

    with col_e:
        st.markdown('<div class="section-title">Distribution des Durées (secondes)</div>', unsafe_allow_html=True)
        df_dist = load_duration_distribution()
        fig5 = px.bar(
            df_dist, x="bucket_min", y="count",
            labels={"bucket_min": "Durée (min)", "count": "Nb Tracks"},
            color="count",
            color_continuous_scale=["#0a3d20", "#1db954", "#7eeba0"],
        )
        fig5.update_layout(**PLOTLY_LAYOUT, height=380,
                           xaxis=dict(**DARK_AXIS),
                           yaxis=dict(**DARK_AXIS),
                           coloraxis_showscale=False)
        st.plotly_chart(fig5, use_container_width=True)

    with col_f:
        st.markdown('<div class="section-title">Durée min/moy/max par Artiste</div>', unsafe_allow_html=True)
        df_dur2 = load_duration_by_artist(min_tracks_artist, top_n)
        fig6 = go.Figure()
        fig6.add_trace(go.Bar(
            name="Min", x=df_dur2["artist_name"], y=df_dur2["min_min"],
            marker_color="#0f5f2b",
        ))
        fig6.add_trace(go.Bar(
            name="Moy", x=df_dur2["artist_name"], y=df_dur2["avg_min"],
            marker_color="#1db954",
        ))
        fig6.add_trace(go.Bar(
            name="Max", x=df_dur2["artist_name"], y=df_dur2["max_min"],
            marker_color="#7eeba0",
        ))
        fig6.update_layout(**PLOTLY_LAYOUT, height=380, barmode="group",
                           legend=dict(font=dict(size=10)),
                           xaxis=dict(**DARK_AXIS, tickangle=-40),
                           yaxis=dict(**DARK_AXIS))
        st.plotly_chart(fig6, use_container_width=True)

    st.markdown('<div class="section-title">Patterns par Position dans la Playlist</div>', unsafe_allow_html=True)
    df_pos = load_position_analysis()
    col_g, col_h = st.columns(2, gap="large")

    with col_g:
        fig7 = px.line(
            df_pos, x="position", y="unique_tracks",
            labels={"position": "Position", "unique_tracks": "Tracks uniques"},
            color_discrete_sequence=["#1db954"],
            markers=True,
        )
        fig7.update_layout(**PLOTLY_LAYOUT, height=300,
                           xaxis=dict(**DARK_AXIS),
                           yaxis=dict(**DARK_AXIS),
                           title=dict(text="Diversité des tracks par position",
                                      font=dict(size=12, color="#888")))
        st.plotly_chart(fig7, use_container_width=True)

    with col_h:
        fig8 = px.line(
            df_pos, x="position", y="avg_duration",
            labels={"position": "Position", "avg_duration": "Durée moy. (min)"},
            color_discrete_sequence=["#52e07c"],
            markers=True,
        )
        fig8.update_layout(**PLOTLY_LAYOUT, height=300,
                           xaxis=dict(**DARK_AXIS),
                           yaxis=dict(**DARK_AXIS),
                           title=dict(text="Durée moyenne selon la position",
                                      font=dict(size=12, color="#888")))
        st.plotly_chart(fig8, use_container_width=True)

# ─── AUTO REFRESH ─────────────────────────────────────────────────────────────
if auto_refresh:
    time.sleep(30)
    st.cache_data.clear()
    st.rerun()