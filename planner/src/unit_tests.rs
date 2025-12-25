use super::*;
use serde_json::to_vec;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

type WorkersList = Arc<Mutex<Vec<Registration>>>;

// ================= JSON =================

#[test]
fn task_json_roundtrip() {
    let task = Task {
        id: 1,
        data: "hello".into(),
    };
    let json = serde_json::to_vec(&task).unwrap();
    let decoded: Task = serde_json::from_slice(&json).unwrap();
    assert_eq!(decoded.id, 1);
}

#[test]
fn task_result_json_roundtrip() {
    let mut wc = HashMap::new();
    wc.insert("rust".into(), 2);
    let result = TaskResult {
        id: 2,
        word_count: wc.clone(),
    };
    let json = serde_json::to_vec(&result).unwrap();
    let decoded: TaskResult = serde_json::from_slice(&json).unwrap();
    assert_eq!(decoded.word_count, wc);
}

#[test]
fn registration_json_roundtrip() {
    let reg = Registration {
        ip: "127.0.0.1".into(),
        port: 9000,
    };
    let json = serde_json::to_vec(&reg).unwrap();
    let decoded: Registration = serde_json::from_slice(&json).unwrap();
    assert_eq!(decoded.port, 9000);
}

// ================= MOCK WORKER =================

async fn start_mock_worker() -> Registration {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 1024 * 1024];
        let n = socket.read(&mut buf).await.unwrap();

        let task: Task = serde_json::from_slice(&buf[..n]).unwrap();
        let mut map = HashMap::new();

        for w in task.data.split_whitespace() {
            *map.entry(w.to_lowercase()).or_insert(0) += 1;
        }

        let result = TaskResult {
            id: task.id,
            word_count: map,
        };
        socket
            .write_all(&serde_json::to_vec(&result).unwrap())
            .await
            .unwrap();
    });

    Registration {
        ip: "127.0.0.1".into(),
        port: addr.port(),
    }
}

// ================= send_task_to_worker =================

#[tokio::test]
async fn send_task_to_worker_success() {
    let worker = start_mock_worker().await;

    let result = send_task_to_worker(worker, 1, "hello rust hello".into())
        .await
        .unwrap();

    assert_eq!(result.word_count["hello"], 2);
    assert_eq!(result.word_count["rust"], 1);
}

// ================= registration listener =================

#[tokio::test]
async fn worker_registration_added() {
    let workers: WorkersList = Arc::new(Mutex::new(Vec::new()));

    // создаём listener на свободном порту
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let workers_clone = workers.clone();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let workers_clone = workers_clone.clone();
            tokio::spawn(async move {
                let mut buffer = vec![0u8; 1024];
                if let Ok(n) = socket.read(&mut buffer).await {
                    if n > 0 {
                        if let Ok(reg) = serde_json::from_slice::<Registration>(&buffer[..n]) {
                            workers_clone.lock().unwrap().push(reg);
                        }
                    }
                }
            });
        }
    });

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let reg = Registration {
        ip: "127.0.0.1".into(),
        port: 7777,
    };
    stream
        .write_all(&serde_json::to_vec(&reg).unwrap())
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let list = workers.lock().unwrap();
    assert_eq!(list.len(), 1);
}

#[tokio::test]
async fn send_task_to_worker_connection_failed() {
    let fake_worker = Registration {
        ip: "127.0.0.1".to_string(),
        port: 65530, // гарантированно нет сервера
    };

    let result = send_task_to_worker(fake_worker, 1, "test".to_string()).await;

    assert!(result.is_none());
}

#[tokio::test]
async fn send_task_to_worker_empty_data() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 1024];
        let n = socket.read(&mut buf).await.unwrap();

        let task: Task = serde_json::from_slice(&buf[..n]).unwrap();
        assert!(task.data.is_empty());

        let result = TaskResult {
            id: task.id,
            word_count: HashMap::new(),
        };

        socket
            .write_all(&serde_json::to_vec(&result).unwrap())
            .await
            .unwrap();
    });

    let worker = Registration {
        ip: "127.0.0.1".to_string(),
        port: addr.port(),
    };

    let result = send_task_to_worker(worker, 1, "".to_string())
        .await
        .unwrap();

    assert!(result.word_count.is_empty());
}

// ================= aggregation logic =================

#[test]
fn aggregation_merges_word_counts_correctly() {
    let mut final_map: HashMap<String, u32> = HashMap::new();

    let r1 = TaskResult {
        id: 1,
        word_count: HashMap::from([("rust".into(), 2), ("tokio".into(), 1)]),
    };

    let r2 = TaskResult {
        id: 2,
        word_count: HashMap::from([("rust".into(), 3), ("async".into(), 1)]),
    };

    for result in [r1, r2] {
        for (k, v) in result.word_count {
            *final_map.entry(k).or_insert(0) += v;
        }
    }

    assert_eq!(final_map["rust"], 5);
    assert_eq!(final_map["tokio"], 1);
    assert_eq!(final_map["async"], 1);
}

// ================= robustness =================

#[tokio::test]
async fn invalid_registration_json_is_ignored() {
    let workers: WorkersList = Arc::new(Mutex::new(Vec::new()));
    let workers_clone = workers.clone();

    tokio::spawn(async move {
        listen_worker_registration(workers_clone).await.unwrap();
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let mut stream = TcpStream::connect("127.0.0.1:8000").await.unwrap();
    stream.write_all(b"INVALID_JSON").await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let list = workers.lock().unwrap();
    assert!(list.is_empty());
}

// ================= multiple registrations =================
#[tokio::test]
async fn multiple_worker_registrations() {
    let workers: WorkersList = Arc::new(Mutex::new(Vec::new()));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let workers_clone = workers.clone();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let workers_clone = workers_clone.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 1024];
                if let Ok(n) = socket.read(&mut buf).await {
                    if n > 0 {
                        if let Ok(reg) = serde_json::from_slice::<Registration>(&buf[..n]) {
                            workers_clone.lock().unwrap().push(reg);
                        }
                    }
                }
            });
        }
    });

    for port in [9001, 9002, 9003] {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let reg = Registration {
            ip: "127.0.0.1".into(),
            port,
        };
        stream.write_all(&to_vec(&reg).unwrap()).await.unwrap();
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    let list = workers.lock().unwrap();
    assert_eq!(list.len(), 3);
}

// ================= concurrent registrations =================
#[tokio::test]
async fn concurrent_worker_registration_safe() {
    let workers: WorkersList = Arc::new(Mutex::new(Vec::new()));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let workers_clone = workers.clone();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let workers_clone = workers_clone.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 1024];
                if let Ok(n) = socket.read(&mut buf).await {
                    if n > 0 {
                        if let Ok(reg) = serde_json::from_slice::<Registration>(&buf[..n]) {
                            workers_clone.lock().unwrap().push(reg);
                        }
                    }
                }
            });
        }
    });

    let mut handles = Vec::new();
    for port in 9100..9110 {
        let addr = addr.clone();
        handles.push(tokio::spawn(async move {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            let reg = Registration {
                ip: "127.0.0.1".into(),
                port,
            };
            stream.write_all(&to_vec(&reg).unwrap()).await.unwrap();
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let list = workers.lock().unwrap();
    assert_eq!(list.len(), 10);
}
