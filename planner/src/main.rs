use axum::{
    Router,
    extract::{Extension, Multipart},
    response::Json,
    routing::post,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tower_http::cors::{Any, CorsLayer};

#[cfg(test)]
mod load_tests;
#[cfg(test)]
mod unit_tests;

#[derive(Serialize, Deserialize, Debug)]
struct Task {
    id: u32,
    data: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct TaskResult {
    id: u32,
    word_count: HashMap<String, u32>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Registration {
    ip: String,
    port: u16,
}

type WorkersList = Arc<Mutex<Vec<Registration>>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let workers: WorkersList = Arc::new(Mutex::new(Vec::new()));

    let workers_clone = workers.clone();
    tokio::spawn(async move {
        listen_worker_registration(workers_clone).await.unwrap();
    });

    let app = Router::new()
        .route("/upload", post(upload_handler))
        .layer(Extension(workers.clone()))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any)
                .allow_credentials(false),
        );

    println!("Planner HTTP сервер на http://127.0.0.1:8080");

    axum::Server::bind(&"127.0.0.1:8080".parse().unwrap())
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

async fn listen_worker_registration(workers: WorkersList) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8000").await?;
    println!("Planner слушает регистрацию воркеров на 127.0.0.1:8000");

    loop {
        let (mut socket, _) = listener.accept().await?;
        let workers_clone = workers.clone();
        tokio::spawn(async move {
            let mut buffer = vec![0u8; 1024];
            if let Ok(n) = socket.read(&mut buffer).await {
                if n > 0 {
                    if let Ok(reg) = serde_json::from_slice::<Registration>(&buffer[..n]) {
                        println!("Зарегистрирован новый Worker: {:?}", reg);
                        workers_clone.lock().unwrap().push(reg);
                    }
                }
            }
        });
    }
}

async fn upload_handler(
    Extension(workers): Extension<WorkersList>,
    mut multipart: Multipart,
) -> Json<HashMap<String, u32>> {
    let mut text_data = String::new();

    while let Some(field_result) = multipart.next_field().await.unwrap() {
        let field: axum::extract::multipart::Field = field_result;

        let data: String = field.text().await.unwrap();
        text_data.push_str(&data);
        text_data.push('\n');
    }

    let workers = workers.lock().unwrap().clone();
    if workers.is_empty() {
        println!("Нет зарегистрированных воркеров!");
        return Json(HashMap::new());
    }

    let chunks: Vec<String> = {
        let lines: Vec<&str> = text_data.lines().collect();
        let chunk_size = (lines.len() + workers.len() - 1) / workers.len();
        lines.chunks(chunk_size).map(|c| c.join("\n")).collect()
    };

    let mut handles = vec![];
    for (i, worker) in workers.iter().enumerate() {
        let chunk = chunks.get(i).cloned().unwrap_or_default();
        let worker = worker.clone();
        handles.push(tokio::spawn(async move {
            send_task_to_worker(worker, i as u32 + 1, chunk).await
        }));
    }

    let mut final_count: HashMap<String, u32> = HashMap::new();
    for handle in handles {
        if let Ok(Some(task_result)) = handle.await {
            for (k, v) in task_result.word_count {
                *final_count.entry(k).or_insert(0) += v;
            }
        }
    }

    Json(final_count)
}

async fn send_task_to_worker(
    worker: Registration,
    task_id: u32,
    data: String,
) -> Option<TaskResult> {
    let task = Task { id: task_id, data };
    let mut stream = TcpStream::connect(format!("{}:{}", worker.ip, worker.port))
        .await
        .ok()?;
    let task_json = serde_json::to_vec(&task).ok()?;
    stream.write_all(&task_json).await.ok()?;

    let mut buffer = vec![0u8; 1024 * 1024];
    let n = stream.read(&mut buffer).await.ok()?;
    let result: TaskResult = serde_json::from_slice(&buffer[..n]).ok()?;
    Some(result)
}
