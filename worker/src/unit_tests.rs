use super::*;
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// ===== UNIT TESTS: count_words =====

#[test]
fn count_words_basic() {
    let text = "hello world hello";
    let result = count_words(text);

    let mut expected = HashMap::new();
    expected.insert("hello".to_string(), 2);
    expected.insert("world".to_string(), 1);

    assert_eq!(result, expected);
}

#[test]
fn count_words_case_insensitive() {
    let text = "Rust rust RUST";
    let result = count_words(text);

    assert_eq!(result["rust"], 3);
}

#[test]
fn count_words_empty_string() {
    let result = count_words("");
    assert!(result.is_empty());
}

#[test]
fn count_words_only_whitespace() {
    let result = count_words("   \n\t   ");
    assert!(result.is_empty());
}

#[test]
fn count_words_multiline() {
    let text = "rust\ntokio\nasync rust";
    let result = count_words(text);

    assert_eq!(result["rust"], 2);
    assert_eq!(result["tokio"], 1);
    assert_eq!(result["async"], 1);
}

#[test]
fn count_words_with_punctuation() {
    let text = "hello, world! hello.";
    let result = count_words(text);

    assert_eq!(result["hello,"], 1);
    assert_eq!(result["hello."], 1);
    assert_eq!(result["world!"], 1);
}

#[test]
fn count_words_unicode() {
    let text = "Привет мир привет";
    let result = count_words(text);

    assert_eq!(result["привет"], 2);
    assert_eq!(result["мир"], 1);
}

// ===== JSON SERIALIZATION TESTS =====

#[test]
fn task_json_roundtrip() {
    let task = Task {
        id: 1,
        data: "hello rust".to_string(),
    };

    let json = serde_json::to_vec(&task).unwrap();
    let decoded: Task = serde_json::from_slice(&json).unwrap();

    assert_eq!(decoded.id, 1);
    assert_eq!(decoded.data, "hello rust");
}

#[test]
fn task_result_json_roundtrip() {
    let mut map = HashMap::new();
    map.insert("rust".to_string(), 10);

    let result = TaskResult {
        id: 42,
        word_count: map.clone(),
    };

    let json = serde_json::to_vec(&result).unwrap();
    let decoded: TaskResult = serde_json::from_slice(&json).unwrap();

    assert_eq!(decoded.id, 42);
    assert_eq!(decoded.word_count, map);
}

#[test]
fn registration_json_roundtrip() {
    let reg = Registration {
        ip: "127.0.0.1".to_string(),
        port: 9001,
    };

    let json = serde_json::to_vec(&reg).unwrap();
    let decoded: Registration = serde_json::from_slice(&json).unwrap();

    assert_eq!(decoded.ip, "127.0.0.1");
    assert_eq!(decoded.port, 9001);
}

// ===== ASYNC TCP TESTS =====

#[tokio::test]
async fn worker_processes_task_correctly() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 1024];

        let n = socket.read(&mut buf).await.unwrap();
        let task: Task = serde_json::from_slice(&buf[..n]).unwrap();

        let result = TaskResult {
            id: task.id,
            word_count: count_words(&task.data),
        };

        socket
            .write_all(&serde_json::to_vec(&result).unwrap())
            .await
            .unwrap();
    });

    let mut client = TcpStream::connect(addr).await.unwrap();

    let task = Task {
        id: 5,
        data: "hello rust hello".to_string(),
    };

    client
        .write_all(&serde_json::to_vec(&task).unwrap())
        .await
        .unwrap();

    let mut buf = vec![0u8; 1024];
    let n = client.read(&mut buf).await.unwrap();

    let result: TaskResult = serde_json::from_slice(&buf[..n]).unwrap();

    assert_eq!(result.id, 5);
    assert_eq!(result.word_count["hello"], 2);
    assert_eq!(result.word_count["rust"], 1);

    server.await.unwrap();
}

#[tokio::test]
async fn worker_handles_empty_task() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 1024];

        let n = socket.read(&mut buf).await.unwrap();
        let task: Task = serde_json::from_slice(&buf[..n]).unwrap();

        let result = TaskResult {
            id: task.id,
            word_count: count_words(&task.data),
        };

        socket
            .write_all(&serde_json::to_vec(&result).unwrap())
            .await
            .unwrap();
    });

    let mut client = TcpStream::connect(addr).await.unwrap();

    let task = Task {
        id: 0,
        data: "".to_string(),
    };

    client
        .write_all(&serde_json::to_vec(&task).unwrap())
        .await
        .unwrap();

    let mut buf = vec![0u8; 1024];
    let n = client.read(&mut buf).await.unwrap();

    let result: TaskResult = serde_json::from_slice(&buf[..n]).unwrap();

    assert_eq!(result.id, 0);
    assert!(result.word_count.is_empty());

    server.await.unwrap();
}

#[tokio::test]
async fn worker_handles_large_input() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 1024 * 1024];

        let n = socket.read(&mut buf).await.unwrap();
        let task: Task = serde_json::from_slice(&buf[..n]).unwrap();

        let result = TaskResult {
            id: task.id,
            word_count: count_words(&task.data),
        };

        socket
            .write_all(&serde_json::to_vec(&result).unwrap())
            .await
            .unwrap();
    });

    let mut client = TcpStream::connect(addr).await.unwrap();

    let text = "rust ".repeat(10_000);
    let task = Task { id: 77, data: text };

    client
        .write_all(&serde_json::to_vec(&task).unwrap())
        .await
        .unwrap();

    let mut buf = vec![0u8; 1024 * 1024];
    let n = client.read(&mut buf).await.unwrap();

    let result: TaskResult = serde_json::from_slice(&buf[..n]).unwrap();

    assert_eq!(result.word_count["rust"], 10_000);

    server.await.unwrap();
}
