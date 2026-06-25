use axum::{
    body::Body,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use futures::{sink::SinkExt, stream::StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io::SeekFrom,
    path::PathBuf,
    process::Stdio,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader},
    process::Command,
    sync::{broadcast, oneshot, Mutex},
};
use tokio_util::io::ReaderStream;
use uuid::Uuid;

// ── Domain types ──────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum JobStatus {
    Queued,
    Downloading,
    Done,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Serialize)]
struct Job {
    id: String,
    url: String,
    status: JobStatus,
    message: String,
    files_done: u32,
    progress: Option<f64>,
    created_at: u64,
    source: String,
}

#[derive(Serialize)]
struct FileEntry {
    name: String,
    size: u64,
    path: String,
}

#[derive(Serialize)]
#[serde(rename_all = "lowercase")]
enum MediaKind {
    Image,
    Video,
}

#[derive(Serialize)]
struct MediaItem {
    url: String,
    path: String,
    name: String,
    /// first path component — used for tab grouping in the UI
    group: String,
    size: u64,
    kind: MediaKind,
}

#[derive(Clone, Serialize, Deserialize)]
struct Favorite {
    id: String,
    url: String,
    kind: String,
    name: String,
    added_at: u64,
}

#[derive(Deserialize)]
struct FavoriteRequest {
    url: String,
    kind: String,
    name: String,
}

/// Persisted to `{download_dir}/{id}.json` so jobs survive restarts.
#[derive(Serialize, Deserialize)]
struct JobMeta {
    id: String,
    url: String,
    source: String,
    created_at: u64,
}

// ── App state ─────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    jobs: Arc<Mutex<HashMap<String, Job>>>,
    cancels: Arc<Mutex<HashMap<String, oneshot::Sender<()>>>>,
    favorites: Arc<Mutex<Vec<Favorite>>>,
    tx: broadcast::Sender<String>,
    download_dir: PathBuf,
}

#[derive(Deserialize)]
struct QueueRequest {
    url: String,
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let download_dir = PathBuf::from(
        std::env::var("DOWNLOAD_DIR").unwrap_or_else(|_| "/downloads".to_string()),
    );
    tokio::fs::create_dir_all(&download_dir).await.unwrap();

    let recovered = load_jobs_from_disk(&download_dir).await;
    let jobs_map: HashMap<String, Job> = recovered.into_iter().map(|j| (j.id.clone(), j)).collect();
    println!("Recovered {} job(s) from disk", jobs_map.len());

    let favorites = load_favorites(&download_dir).await;

    let (tx, _) = broadcast::channel::<String>(512);
    let state = AppState {
        jobs: Arc::new(Mutex::new(jobs_map)),
        cancels: Arc::new(Mutex::new(HashMap::new())),
        favorites: Arc::new(Mutex::new(favorites)),
        tx,
        download_dir,
    };

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/api/queue", post(queue_handler))
        .route("/api/jobs", get(jobs_handler))
        .route("/api/jobs/:id/cancel", post(cancel_handler))
        .route("/api/jobs/:id/files", get(files_handler))
        .route("/api/jobs/:id/media", get(media_handler))
        .route("/api/favorites", get(list_favs).post(add_fav))
        .route("/api/favorites/:id", delete(remove_fav))
        .route("/files/:id/*file_path", get(serve_file))
        .route("/thumbs/:id/*file_path", get(thumb_handler))
        .route("/ws", get(ws_handler))
        .with_state(state);

    let addr = "0.0.0.0:3000";
    println!("Listening on http://{addr}");
    axum::serve(tokio::net::TcpListener::bind(addr).await.unwrap(), app)
        .await
        .unwrap();
}

// ── HTTP handlers ─────────────────────────────────────────────────────────────

async fn index_handler() -> impl IntoResponse {
    axum::response::Html(include_str!("index.html"))
}

async fn jobs_handler(State(state): State<AppState>) -> impl IntoResponse {
    let jobs: Vec<Job> = state.jobs.lock().await.values().cloned().collect();
    Json(jobs)
}

async fn queue_handler(
    State(state): State<AppState>,
    Json(req): Json<QueueRequest>,
) -> impl IntoResponse {
    let url = req.url.trim().to_string();
    if url.is_empty() {
        return StatusCode::BAD_REQUEST;
    }
    let source = detect_source(&url).to_string();
    let id = Uuid::new_v4().to_string();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let job = Job {
        id: id.clone(),
        url,
        status: JobStatus::Queued,
        message: String::new(),
        files_done: 0,
        progress: None,
        created_at: now,
        source,
    };
    state.jobs.lock().await.insert(id.clone(), job.clone());
    save_job_meta(&state.download_dir, &job).await;
    broadcast_job(&state, &job);
    tokio::spawn(run_download(state, id));
    StatusCode::OK
}

async fn cancel_handler(
    Path(id): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    match state.cancels.lock().await.remove(&id) {
        Some(tx) => {
            let _ = tx.send(());
            StatusCode::OK
        }
        None => StatusCode::NOT_FOUND,
    }
}

async fn files_handler(
    Path(id): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let dest = state.download_dir.join(&id);
    match list_all_files(&dest).await {
        Ok(mut files) => {
            files.sort_by(|a, b| a.path.cmp(&b.path));
            Json(files).into_response()
        }
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn media_handler(
    Path(id): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let base = state.download_dir.join(&id);
    let source = state.jobs.lock().await.get(&id).map(|j| j.source.clone()).unwrap_or_default();
    let skip_root = source == "mega";
    match collect_media(&base, &id, skip_root).await {
        Ok(mut items) => {
            items.sort_by(|a, b| a.path.cmp(&b.path));
            Json(items).into_response()
        }
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

/// Serves downloaded files with Range request support so videos can be seeked.
async fn serve_file(
    Path((id, file_path)): Path<(String, String)>,
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Response {
    let base = state.download_dir.join(&id);
    let full = base.join(&file_path);

    if !full.starts_with(&base) {
        return StatusCode::FORBIDDEN.into_response();
    }

    let mut file = match tokio::fs::File::open(&full).await {
        Ok(f) => f,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };

    let file_size = match file.metadata().await {
        Ok(m) => m.len(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let ext = full
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    let content_type = mime_for_ext(&ext);

    if let Some((start, end)) = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| parse_range(s, file_size))
    {
        let length = end - start + 1;
        if file.seek(SeekFrom::Start(start)).await.is_err() {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
        let stream = ReaderStream::new(file.take(length));
        return Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header(header::CONTENT_TYPE, content_type)
            .header(
                header::CONTENT_RANGE,
                format!("bytes {start}-{end}/{file_size}"),
            )
            .header(header::CONTENT_LENGTH, length.to_string())
            .header(header::ACCEPT_RANGES, "bytes")
            .body(Body::from_stream(stream))
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
    }

    let stream = ReaderStream::new(file);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_LENGTH, file_size.to_string())
        .header(header::ACCEPT_RANGES, "bytes")
        .body(Body::from_stream(stream))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// Returns a thumbnail JPEG for any media file. Generated once via ffmpeg and
/// cached permanently in `{job_dir}/.thumbs/{rel_path}.jpg`.
async fn thumb_handler(
    Path((id, file_path)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Response {
    let base = state.download_dir.join(&id);
    let src  = base.join(&file_path);

    if !src.starts_with(&base) {
        return StatusCode::FORBIDDEN.into_response();
    }

    // Cache: {base}/.thumbs/{file_path}.jpg
    let thumb = base.join(".thumbs").join(&file_path).with_extension("jpg");

    // Serve from disk if already generated
    if let Ok(bytes) = tokio::fs::read(&thumb).await {
        return (
            [
                (header::CONTENT_TYPE,  "image/jpeg"),
                (header::CACHE_CONTROL, "max-age=31536000, immutable"),
            ],
            bytes,
        )
            .into_response();
    }

    // Ensure parent directory exists
    if let Some(parent) = thumb.parent() {
        tokio::fs::create_dir_all(parent).await.ok();
    }

    let ext = src.extension().and_then(|e| e.to_str()).unwrap_or("").to_lowercase();
    let is_video = matches!(
        ext.as_str(),
        "mp4" | "webm" | "mov" | "mkv" | "avi" | "m4v" | "3gp"
    );

    // Build ffmpeg command. For videos, seek to 1 s before opening the file
    // so ffmpeg doesn't have to decode from the start.
    let mut cmd = Command::new("ffmpeg");
    cmd.arg("-y");
    if is_video {
        cmd.args(["-ss", "1"]);
    }
    cmd.args([
        "-i",
        src.to_str().unwrap(),
        "-vframes",
        "1",
        "-vf",
        "scale=400:400:force_original_aspect_ratio=decrease",
        "-q:v",
        "4",
        thumb.to_str().unwrap(),
    ])
    .stdout(Stdio::null())
    .stderr(Stdio::null());

    match cmd.status().await {
        Ok(s) if s.success() => match tokio::fs::read(&thumb).await {
            Ok(bytes) => (
                [
                    (header::CONTENT_TYPE,  "image/jpeg"),
                    (header::CACHE_CONTROL, "max-age=31536000, immutable"),
                ],
                bytes,
            )
                .into_response(),
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        },
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

// ── Favorites handlers ────────────────────────────────────────────────────────

async fn list_favs(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.favorites.lock().await.clone())
}

async fn add_fav(
    State(state): State<AppState>,
    Json(req): Json<FavoriteRequest>,
) -> impl IntoResponse {
    let fav = Favorite {
        id: Uuid::new_v4().to_string(),
        url: req.url,
        kind: req.kind,
        name: req.name,
        added_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    };
    let mut favs = state.favorites.lock().await;
    favs.push(fav.clone());
    save_favorites(&state.download_dir, &favs).await;
    Json(fav)
}

async fn remove_fav(
    Path(id): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let mut favs = state.favorites.lock().await;
    let before = favs.len();
    favs.retain(|f| f.id != id);
    if favs.len() < before {
        save_favorites(&state.download_dir, &favs).await;
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

// ── WebSocket ─────────────────────────────────────────────────────────────────

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();

    let jobs: Vec<Job> = state.jobs.lock().await.values().cloned().collect();
    let snapshot = serde_json::json!({"type": "snapshot", "jobs": jobs}).to_string();
    if sender.send(Message::Text(snapshot)).await.is_err() {
        return;
    }

    let mut rx = state.tx.subscribe();
    loop {
        tokio::select! {
            msg = rx.recv() => match msg {
                Ok(text) => { if sender.send(Message::Text(text)).await.is_err() { break; } }
                Err(_) => break,
            },
            msg = receiver.next() => match msg {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                _ => {}
            },
        }
    }
}

// ── Download orchestration ────────────────────────────────────────────────────

async fn run_download(state: AppState, id: String) {
    let (url, source, dest) = {
        let jobs = state.jobs.lock().await;
        let job = jobs.get(&id).unwrap();
        (
            job.url.clone(),
            job.source.clone(),
            state.download_dir.join(&id),
        )
    };

    tokio::fs::create_dir_all(&dest).await.ok();

    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
    state.cancels.lock().await.insert(id.clone(), cancel_tx);

    {
        let mut jobs = state.jobs.lock().await;
        if let Some(job) = jobs.get_mut(&id) {
            job.status = JobStatus::Downloading;
            job.message = "Starting...".to_string();
            broadcast_job(&state, job);
        }
    }

    let result = match source.as_str() {
        "mega" => download_mega(&url, &dest, &state, &id, cancel_rx).await,
        _ => download_gallery_dl(&url, &dest, &state, &id, cancel_rx).await,
    };

    state.cancels.lock().await.remove(&id);

    {
        let mut jobs = state.jobs.lock().await;
        if let Some(job) = jobs.get_mut(&id) {
            match result {
                Ok(_) => {
                    job.status = JobStatus::Done;
                    job.progress = Some(100.0);
                    job.message = "Complete".to_string();
                }
                Err(ref e) if e == "Cancelled" => {
                    job.status = JobStatus::Cancelled;
                    job.message = "Cancelled".to_string();
                }
                Err(e) => {
                    job.status = JobStatus::Failed;
                    job.message = e;
                }
            }
            broadcast_job(&state, job);
        }
    }
}

async fn download_gallery_dl(
    url: &str,
    dest: &PathBuf,
    state: &AppState,
    id: &str,
    mut cancel_rx: oneshot::Receiver<()>,
) -> Result<(), String> {
    let mut child = Command::new("gallery-dl")
        .args(["-D", dest.to_str().unwrap(), "--no-mtime", url])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("gallery-dl not found: {e}"))?;

    let mut out = BufReader::new(child.stdout.take().unwrap()).lines();
    let mut err = BufReader::new(child.stderr.take().unwrap()).lines();
    let mut cancelled = false;

    loop {
        tokio::select! {
            biased;
            _ = &mut cancel_rx => { let _ = child.kill().await; cancelled = true; break; }
            line = out.next_line() => match line {
                Ok(Some(l)) if !l.trim().is_empty() => {
                    let mut jobs = state.jobs.lock().await;
                    if let Some(job) = jobs.get_mut(id) {
                        if let Some((cur, tot)) = parse_gallery_dl_counter(&l) {
                            job.files_done = cur;
                            job.progress = Some(cur as f64 / tot as f64 * 100.0);
                        } else if l.contains("Downloading") {
                            job.files_done += 1;
                        }
                        job.message = truncate(l.trim(), 80);
                        broadcast_job(state, job);
                    }
                }
                Ok(None) => break,
                _ => {}
            },
            line = err.next_line() => match line {
                Ok(Some(l)) if !l.trim().is_empty() => {
                    let mut jobs = state.jobs.lock().await;
                    if let Some(job) = jobs.get_mut(id) {
                        job.message = truncate(l.trim(), 80);
                        broadcast_job(state, job);
                    }
                }
                _ => {}
            },
        }
    }

    if cancelled {
        return Err("Cancelled".to_string());
    }
    let status = child.wait().await.map_err(|e| e.to_string())?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("gallery-dl exited with {}", status))
    }
}

async fn download_mega(
    url: &str,
    dest: &PathBuf,
    state: &AppState,
    id: &str,
    mut cancel_rx: oneshot::Receiver<()>,
) -> Result<(), String> {
    let mut child = Command::new("megadl")
        .args(["--path", dest.to_str().unwrap(), url])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("megatools not found: {e}"))?;

    let mut out = BufReader::new(child.stdout.take().unwrap()).lines();
    let mut err = BufReader::new(child.stderr.take().unwrap()).lines();
    let mut cancelled = false;

    loop {
        tokio::select! {
            biased;
            _ = &mut cancel_rx => { let _ = child.kill().await; cancelled = true; break; }
            line = out.next_line() => match line {
                Ok(Some(l)) if !l.trim().is_empty() => {
                    let mut jobs = state.jobs.lock().await;
                    if let Some(job) = jobs.get_mut(id) {
                        if let Some(pct) = parse_mega_progress(&l) {
                            job.progress = Some(pct);
                        }
                        if l.contains("Downloading") {
                            job.files_done += 1;
                        }
                        job.message = truncate(l.trim(), 80);
                        broadcast_job(state, job);
                    }
                }
                Ok(None) => break,
                _ => {}
            },
            line = err.next_line() => match line {
                Ok(Some(l)) if !l.trim().is_empty() => {
                    let mut jobs = state.jobs.lock().await;
                    if let Some(job) = jobs.get_mut(id) {
                        job.message = truncate(l.trim(), 80);
                        broadcast_job(state, job);
                    }
                }
                _ => {}
            },
        }
    }

    if cancelled {
        return Err("Cancelled".to_string());
    }
    let status = child.wait().await.map_err(|e| e.to_string())?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("megadl exited with {}", status))
    }
}

// ── File / media helpers ──────────────────────────────────────────────────────

async fn list_all_files(base: &PathBuf) -> std::io::Result<Vec<FileEntry>> {
    let mut out = Vec::new();
    let mut stack = vec![base.clone()];
    while let Some(dir) = stack.pop() {
        let mut rd = match tokio::fs::read_dir(&dir).await {
            Ok(r) => r,
            Err(_) => continue,
        };
        while let Ok(Some(entry)) = rd.next_entry().await {
            let meta = match entry.metadata().await {
                Ok(m) => m,
                Err(_) => continue,
            };
            let path = entry.path();
            if meta.is_dir() {
                stack.push(path);
            } else {
                let rel = path.strip_prefix(base).unwrap_or(&path);
                out.push(FileEntry {
                    name: entry.file_name().to_string_lossy().to_string(),
                    size: meta.len(),
                    path: rel.to_string_lossy().to_string(),
                });
            }
        }
    }
    Ok(out)
}

/// Recursively collects image/video files, skipping anything at the root level.
async fn collect_media(base: &PathBuf, job_id: &str, skip_root: bool) -> std::io::Result<Vec<MediaItem>> {
    let mut items = Vec::new();
    // stack: (directory path, relative prefix)
    let mut stack = vec![(base.clone(), String::new())];

    while let Some((dir, prefix)) = stack.pop() {
        let mut rd = match tokio::fs::read_dir(&dir).await {
            Ok(r) => r,
            Err(_) => continue,
        };
        while let Ok(Some(entry)) = rd.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            let rel = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}/{name}")
            };
            let meta = match entry.metadata().await {
                Ok(m) => m,
                Err(_) => continue,
            };
            if meta.is_dir() {
                stack.push((entry.path(), rel));
            } else if let Some(kind) = media_kind(&name) {
                // Mega stores files in subfolders; skip root-level files for those.
                // Instagram/gallery-dl writes directly to the job root, so include everything.
                if skip_root && !rel.contains('/') {
                    continue;
                }
                let group = if rel.contains('/') {
                    rel.split('/').next().unwrap_or("").to_string()
                } else {
                    String::new()
                };
                items.push(MediaItem {
                    url: format!("/files/{job_id}/{rel}"),
                    path: rel,
                    name,
                    group,
                    size: meta.len(),
                    kind,
                });
            }
        }
    }
    Ok(items)
}

fn media_kind(name: &str) -> Option<MediaKind> {
    let ext = name.rsplit('.').next()?.to_lowercase();
    match ext.as_str() {
        "jpg" | "jpeg" | "png" | "gif" | "webp" | "avif" | "heic" | "heif" => {
            Some(MediaKind::Image)
        }
        "mp4" | "webm" | "mov" | "mkv" | "avi" | "m4v" | "3gp" => Some(MediaKind::Video),
        _ => None,
    }
}

fn mime_for_ext(ext: &str) -> &'static str {
    match ext {
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "avif" => "image/avif",
        "mp4" | "m4v" => "video/mp4",
        "webm" => "video/webm",
        "mov" => "video/quicktime",
        "mkv" => "video/x-matroska",
        _ => "application/octet-stream",
    }
}

/// Parses a `bytes=start-end` Range header.
fn parse_range(s: &str, size: u64) -> Option<(u64, u64)> {
    let s = s.strip_prefix("bytes=")?;
    let (a, b) = s.split_once('-')?;
    let start: u64 = a.trim().parse().ok()?;
    let end: u64 = if b.trim().is_empty() {
        size.saturating_sub(1)
    } else {
        b.trim().parse().ok()?
    };
    if start > end || end >= size {
        return None;
    }
    Some((start, end))
}

// ── Persistence ───────────────────────────────────────────────────────────────

async fn save_job_meta(download_dir: &PathBuf, job: &Job) {
    let meta = JobMeta {
        id: job.id.clone(),
        url: job.url.clone(),
        source: job.source.clone(),
        created_at: job.created_at,
    };
    if let Ok(json) = serde_json::to_string(&meta) {
        let _ = tokio::fs::write(download_dir.join(format!("{}.json", job.id)), json).await;
    }
}

/// On startup, scan `download_dir` for `{id}.json` metadata files and reconstruct
/// completed jobs so previously downloaded folders are visible immediately.
async fn load_jobs_from_disk(download_dir: &PathBuf) -> Vec<Job> {
    let mut jobs = Vec::new();
    let mut rd = match tokio::fs::read_dir(download_dir).await {
        Ok(r) => r,
        Err(_) => return jobs,
    };

    while let Ok(Some(entry)) = rd.next_entry().await {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }

        let content = match tokio::fs::read_to_string(&path).await {
            Ok(c) => c,
            Err(_) => continue,
        };
        let meta: JobMeta = match serde_json::from_str(&content) {
            Ok(m) => m,
            Err(_) => continue,
        };

        // Only surface the job if the download folder actually exists
        let job_dir = download_dir.join(&meta.id);
        if !tokio::fs::try_exists(&job_dir).await.unwrap_or(false) {
            continue;
        }

        let files_done = collect_media(&job_dir, &meta.id, meta.source == "mega")
            .await
            .map(|v| v.len() as u32)
            .unwrap_or(0);

        jobs.push(Job {
            id: meta.id,
            url: meta.url,
            source: meta.source,
            created_at: meta.created_at,
            status: JobStatus::Done,
            message: String::new(),
            files_done,
            progress: Some(100.0),
        });
    }

    jobs
}

async fn load_favorites(download_dir: &PathBuf) -> Vec<Favorite> {
    tokio::fs::read_to_string(download_dir.join("favorites.json"))
        .await
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

async fn save_favorites(download_dir: &PathBuf, favs: &[Favorite]) {
    if let Ok(json) = serde_json::to_string(favs) {
        let _ = tokio::fs::write(download_dir.join("favorites.json"), json).await;
    }
}

// ── Misc helpers ──────────────────────────────────────────────────────────────

fn detect_source(url: &str) -> &'static str {
    if url.contains("mega.nz") || url.contains("mega.co.nz") {
        "mega"
    } else if url.contains("instagram.com") || url.contains("instagr.am") {
        "instagram"
    } else {
        "unknown"
    }
}

fn broadcast_job(state: &AppState, job: &Job) {
    let _ = state
        .tx
        .send(serde_json::json!({"type": "update", "job": job}).to_string());
}

fn parse_gallery_dl_counter(line: &str) -> Option<(u32, u32)> {
    let start = line.rfind("][")?;
    let inner = line[start + 2..].split(']').next()?;
    let (a, b) = inner.split_once('/')?;
    Some((a.trim().parse().ok()?, b.trim().parse().ok()?))
}

fn parse_mega_progress(line: &str) -> Option<f64> {
    let idx = line.find("Progress:")?;
    let rest = line[idx + 9..].trim();
    let end = rest.find('%')?;
    rest[..end].trim().parse().ok()
}

fn truncate(s: &str, max: usize) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= max {
        s.to_string()
    } else {
        format!("{}…", chars[..max].iter().collect::<String>())
    }
}
